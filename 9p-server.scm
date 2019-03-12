;;; 9p-server.scm
;
;; An implementation of the Plan 9 File Protocol (9p)
;; This egg implements the version known as 9p2000 or Styx.
;;
;; This file contains the apparatus required to be a Plan 9
;; server.
;
; Copyright (c) 2012, Alaric Snell-Pym
; All rights reserved.
;
; Redistribution and use in source and binary forms, with or without
; modification, are permitted provided that the following conditions
; are met:
;
; 1. Redistributions of source code must retain the above copyright
;    notice, this list of conditions and the following disclaimer.
; 2. Redistributions in binary form must reproduce the above copyright
;    notice, this list of conditions and the following disclaimer in the
;    documentation and/or other materials provided with the distribution.
; 3. Neither the name of the author nor the names of its
;    contributors may be used to endorse or promote products derived
;    from this software without specific prior written permission.
;
; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
; "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
; LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
; FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
; COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
; INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
; (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
; SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
; HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
; STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
; ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
; OF THE POSSIBILITY OF SUCH DAMAGE.
;
; Please report bugs, suggestions and ideas to the Chicken Trac
; ticket tracking system (assign tickets to user 'alaric'):
; http://trac.callcc.org

(require-library srfi-18 srfi-69 9p-lolevel extras)

(module 9p-server
 (serve)

 (import scheme chicken srfi-18 srfi-69 (prefix 9p-lolevel 9p:) extras)

 (define session-error-message "A session has not been initiated with a Tversion request")

 (define (dbg message . args)
  ;;   (apply printf message args)
  ;;   (newline)
   (if (pair? args)
       (car args)
       (void)))

 ;; This procedure calls the given callbacks when different requests
 ;; arrive. However, you are responsible for sending responses
 ;; yourself, beyond the special case of Tversion.

 ;; Types of handlers:

 ;; (handle-version message) => max-size
 ;; (handle-auth message bind-fid! reply! error!) => <unspecified>
 ;; (handle-flush message reply! error!) => <unspecified>
 ;; (handle-attach message auth-fid-value bind-fid! reply! error!) => <unspecified>
 ;; (handle-walk message parent-fid-value bind-fid! reply! error!) => <unspecified>
 ;; (handle-open message fid-value reply! error!) => <unspecified>
 ;; (handle-create message fid-value reply! error!) => <unspecified>
 ;; (handle-read message fid-value reply! error!) => <unspecified>
 ;; (handle-write message fid-value reply! error!) => <unspecified>
 ;; (handle-clunk fid-value reply! error!) => <unspecified>
 ;; (handle-remove fid-value reply! error!) => <unspecified>
 ;; (handle-stat message fid-value reply! error!) => <unspecified>
 ;; (handle-wstat message fid-value reply! error!) => <unspecified>

 ;; Types used in types of handlers:

 ;; message     The contents field of a 9p message (a list)
 ;; (bind-fid! obj) => <unspecified>   Binds the given arbitrary value to the applicable fid.
 ;; (reply! message) => <unspecified>   Sends the supplied message contents as the success response
 ;; (error! string) => <unspecified>    Sends the supplied error message as a failure response

 ;; FIXME: Add exception catching when we invoked handlers and call
 ;;  error! with the exn message rather than leaving the request dangling.

 (define (serve-9p2000 inport outport handle-version handle-auth handle-flush handle-attach handle-walk handle-open handle-create handle-read handle-write handle-clunk handle-remove handle-stat handle-wstat handle-disconnect)
   (let* ((fids-mutex (make-mutex))
          (fids (make-hash-table))
          (lookup-fid (lambda (fid)
                        (dynamic-wind
                            (lambda () (mutex-lock! fids-mutex))
                            (lambda () (hash-table-ref/default fids fid #f))
                            (lambda () (mutex-unlock! fids-mutex)))))
          (bind-fid! (lambda (fid value)
                       (dbg "Binding ~S to fid ~S" value fid)
                       (dynamic-wind
                           (lambda () (mutex-lock! fids-mutex))
                           (lambda () (hash-table-set! fids fid value))
                           (lambda () (mutex-unlock! fids-mutex)))
                       (void)))
          (clunk-fid! (lambda (fid)
                        (dbg "Clunking fid ~S" fid)
                        (dynamic-wind
                            (lambda () (mutex-lock! fids-mutex))
                            (lambda () (hash-table-delete! fids fid))
                            (lambda () (mutex-unlock! fids-mutex)))
                        (void)))
          (outport-mutex (make-mutex))
          (send-message! (lambda (msg)
                           (dbg "Sending message ~S/~S/~S"
                                (9p:message-type msg)
                                (9p:message-tag msg)
                                (9p:message-contents msg))
                           (dynamic-wind
                               (lambda () (mutex-lock! outport-mutex))
                               (lambda ()
                                 (9p:send-message outport msg)
                                 (flush-output outport))
                               (lambda () (mutex-unlock! outport-mutex)))
                           (void)))
          (send-error! (lambda (msg error)
                        (send-message!
                         (9p:make-message 'Rerror
                                          (9p:message-tag msg)
                                          (list error)))))
          (call-standard-handler
           ;; This calls a handler that involves no FIDs
           (lambda (handler message Rtype)
             (handler (9p:message-contents message)
                      (lambda (reply)
                        (send-message!
                         (9p:make-message Rtype (9p:message-tag message) reply)))
                      (lambda (error)
                        (send-error! message error)))))
          (call-fiddly-handler
           ;; This calls a handler that binds a FID supplied as the
           ;; first argument in the message
           (lambda (handler message Rtype)
             (let ((fid (car (9p:message-contents message))))
               (handler (9p:message-contents message)
                        (lambda (val) (bind-fid! fid val))
                        (lambda (reply)
                          (send-message!
                           (9p:make-message Rtype (9p:message-tag message) reply)))
                        (lambda (error)
                          (send-error! message error))))))
          (call-fid-using-handler
           ;; This calls a handler that has an existing FID as the first
           ;; argument in the message
           (lambda (handler message Rtype)
             (let ((fid (car (9p:message-contents message))))
               (handler (9p:message-contents message)
                        (lookup-fid fid)
                        (lambda (reply)
                          (send-message!
                           (9p:make-message Rtype (9p:message-tag message) reply)))
                        (lambda (error)
                          (send-error! message error)))))))
     (let loop ((session-started? #f))
       (dbg "Waiting for message")
       (let ((message (9p:receive-message inport)))
         (if (eof-object? message)
             (handle-disconnect)
             (begin
              (dbg "Received a message ~S/~S/~S"
                   (9p:message-type message)
                   (9p:message-tag message)
                   (9p:message-contents message))
              (case (9p:message-type message)
                ((Tversion)
                 (let ((max-size (min
                                  (car (9p:message-contents message))
                                  (handle-version (9p:message-contents message)))))
                   (send-message!
                    (9p:make-message 'Rversion (9p:message-tag message) (list max-size "9P2000")))
                   (loop #t)))
                ((Tauth)
                 (if session-started?
                     (call-fiddly-handler handle-auth message 'Rauth)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tflush)
                 (if session-started?
                     (call-standard-handler handle-flush message 'Rflush)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tattach)
                 (if session-started?
                     (let ((root-fid (car (9p:message-contents message)))
                           (auth-fid (cadr (9p:message-contents message))))
                       (handle-attach (9p:message-contents message)
                                      (lookup-fid auth-fid)
                                      (lambda (val) (bind-fid! root-fid val))
                                      (lambda (reply)
                                        (send-message!
                                         (9p:make-message 'Rattach (9p:message-tag message) reply)))
                                      (lambda (error)
                                        (send-error! message error))))
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Twalk)
                 (if session-started?
                     (let ((parent-fid (car (9p:message-contents message)))
                           (child-fid (cadr (9p:message-contents message))))
                       (handle-walk (9p:message-contents message)
                                    (lookup-fid parent-fid)
                                    (lambda (val) (bind-fid! child-fid val))
                                    (lambda (reply)
                                      (send-message!
                                       (9p:make-message 'Rwalk (9p:message-tag message) reply)))
                                    (lambda (error)
                                      (send-error! message error))))
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Topen)
                 (if session-started?
                     (call-fid-using-handler handle-open message 'Ropen)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tcreate)
                 (if session-started?
                     (call-fid-using-handler handle-create message 'Rcreate)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tread)
                 (if session-started?
                     (call-fid-using-handler handle-read message 'Rread)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Twrite)
                 (if session-started?
                     (call-fid-using-handler handle-write message 'Rwrite)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tclunk)
                 (if session-started?
                     (let* ((fid (car (9p:message-contents message)))
                            (fid-value (lookup-fid fid)))
                       (clunk-fid! fid)
                       (handle-clunk
                        fid-value
                        (lambda (reply)
                          (send-message!
                           (9p:make-message 'Rclunk (9p:message-tag message) reply)))
                        (lambda (error)
                          (send-error! message error))))
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tremove)
                 (if session-started?
                     (let* ((fid (car (9p:message-contents message)))
                            (fid-value (lookup-fid fid)))
                       (handle-remove
                        fid-value
                        (lambda (reply)
                          (send-message!
                           (9p:make-message 'Rremove (9p:message-tag message) reply)))
                        (lambda (error)
                          (send-error! message error))))
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Tstat)
                 (if session-started?
                     (call-fid-using-handler handle-stat message 'Rstat)
                     (send-error! message session-error-message))
                 (loop session-started?))
                ((Twstat)
                 (if session-started?
                     (call-fid-using-handler handle-wstat message 'Rwstat)
                     (send-error! message session-error-message))
                 (loop session-started?)))))))))


 (define (serve inport outport handlers)
   (let ((assq* (lambda (obj a-list)
                  (let ((elem (assq obj a-list)))
                    (if elem
                        (cdr elem)
                        #f)))))
     (let ((handle-version (or (assq* 'version handlers)
                               (lambda (message) (car message))))
           (handle-auth (or (assq* 'auth handlers)
                            (lambda (message bind-fid! reply! error!)
                              (error! "Authentication is not supported"))))
           (handle-flush (or (assq* 'flush handlers)
                             (lambda (message reply! error!)
                               (reply! '()))))
           (handle-attach (or (assq* 'attach handlers)
                              (error "attach must be implemented")))
           (handle-walk (or (assq* 'walk handlers)
                            (error "walk must be implemented")))
           (handle-open (or (assq* 'open handlers)
                            (error "open must be implemented")))
           (handle-create (or (assq* 'create handlers)
                              (lambda (message fid-value reply! error!)
                                (error! "Creating objects is not supported"))))
           (handle-read (or (assq* 'read handlers)
                            (lambda (message fid-value reply! error!)
                              (error! "Reading is not supported"))))
           (handle-write (or (assq* 'write handlers)
                             (lambda (message fid-value reply! error!)
                               (error! "Writing is not supported"))))
           (handle-clunk (or (assq* 'clunk handlers)
                             (lambda (fid-value reply! error!)
                               (reply! '()))))
           (handle-remove (or (assq* 'remove handlers)
                              (lambda (fid-value reply! error!)
                                (error! "Removing objects is not supported"))))
           (handle-stat (or (assq* 'stat handlers)
                            (lambda (message fid-value reply! error!)
                              (error! "Stat is not supported"))))
           (handle-wstat (or (assq* 'wstat handlers)
                             (lambda (message fid-value reply! error!)
                               (error! "Wstat is not supported"))))
           (handle-disconnect (or (assq* 'disconnect handlers)
                                  (lambda ()
                                    (void)))))

       (serve-9p2000 inport outport
                     handle-version handle-auth handle-flush
                     handle-attach handle-walk handle-open
                     handle-create handle-read handle-write
                     handle-clunk handle-remove handle-stat
                     handle-wstat handle-disconnect))))
)