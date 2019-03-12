;;;; 9p-lolevel.scm
;
;; An implementation of the Plan 9 File Protocol (9p)
;; This egg implements the version known as 9p2000 or Styx.
;;
;; This file contains the dirty low-level stuff like network byte
;; packing and the actual transmission and receival of messages.
;
; Copyright (c) 2012, Peter Bex & Alaric Snell-Pym
; Copyright (c) 2007, 2008, 2011 Peter Bex
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
; ticket tracking system (assign tickets to user 'sjamaan' or 'alaric'):
; http://trac.callcc.org
;
; This implementation is based on what passes for a "specification",
; the manpages at http://plan9.bell-labs.com/sys/man/5/INDEX.html
; Another useful resource is http://9p.cat-v.org/documentation/

;; Notes:
;; Perhaps a dyn-vector can be used instead of lists of u8vectors.
;; Possibly this is more efficient.

(module 9p-lolevel
 (qid? make-qid qid-type qid-version qid-path
  qid-type-set! qid-version-set! qid-path-set!
  open/rdonly open/wronly open/rdwr open/trunc open/rclose
  perm/ixoth perm/iwoth perm/iroth
  perm/ixusr perm/iwusr perm/irusr
  perm/ixgrp perm/iwgrp perm/irgrp
  dmdir dmappend dmexcl dmauth dmtmp
  qtfile qtdir qtappend qtexcl qtauth qttmp
  notag nofid stat-keep-number stat-keep-string
  message? make-message message-type message-tag message-contents
  send-message receive-message data->directory-listing
  data->full-directory-listing full-directory-listing->data
  message-type-set! message-contents-set! message-tag-set!)

(import scheme chicken)
(use srfi-1 srfi-4 extras lolevel)

(define-record qid
  
  type version path)

;; Open flags
(define open/rdonly #x00)
(define open/wronly #x01)
(define open/rdwr #x02)
(define open/trunc #x10)
(define open/rclose #x40) ;; Remove/unlink on clunk/close

;; Note that for Unix systems these permissions are the same (?).
;; For Windows system these may not be the same.  In any case, we don't
;; want to make assumptions about these things.
(define perm/ixoth #o001)
(define perm/iwoth #o002)
(define perm/iroth #o004)
(define perm/ixgrp #o010)
(define perm/iwgrp #o020)
(define perm/irgrp #o040)
(define perm/ixusr #o100)
(define perm/iwusr #o200)
(define perm/irusr #o400)

(define dmdir    #x80000000) ; Is a directory
(define dmappend #x40000000) ; Append-only
(define dmexcl   #x20000000) ; Exclusive use
; #x10000000 is skipped "for historical reasons"
(define dmauth   #x08000000) ; Authentication file (established by auth messages)
(define dmtmp    #x04000000) ; Temporary file

(define qtfile   #x00) ; Don't check for this!
(define qtdir    #x80)
(define qtappend #x40)
(define qtexcl   #x20)
; #x10 is skipped "for historical reasons"
(define qtauth   #x08)
(define qttmp    #x04)

(define notag #xffff)      ;; For Tversion
(define nofid #xffffffff)  ;; For Tattach

;; For Twstat messages, when the server should keep the current value
;; (aka "don't touch" in the manpage)
(define stat-keep-number #xffffffff)
(define stat-keep-string "")

(define message-types
  `((Tversion msize string)
    (Rversion msize string)
    (Tauth    fid string string)
    (Rauth    qid)
    (Tattach  fid fid string string)
    (Rattach  qid)
    (Terror   )
    (Rerror   string)
    (Tflush   tag)
    (Rflush   )
    (Twalk    fid fid (string))
    (Rwalk    (qid))
    (Topen    fid access-mode)
    (Ropen    qid msize)
    (Tcreate  fid string permission-mode access-mode)
    (Rcreate  qid msize)
    (Tread    fid filesize datasize)
    (Rread    data)
    (Twrite   fid filesize data)
    (Rwrite   datasize)
    (Tclunk   fid)
    (Rclunk   )
    (Tremove  fid)
    (Rremove  )
    (Tstat    fid)
    ;; XXX Double statsize is a bit weird. See the BUGS section in
    ;; stat(9) for something that is supposed to pass for an explanation
    (Rstat    statsize statsize type dev qid permission-mode time time filesize string string string string)
    (Twstat   fid statsize statsize type dev qid permission-mode time time filesize string string string string) ;; Untested!
    (Rwstat)))

;; These vectors are in reverse network byte ordering because the 9p protocol
;; expects them that way.  (ie, little endian)
(define (u8vector->number v)
  (let loop ((i (u8vector-length v))
	     (num 0))
    (if (zero? i)
	num
        (loop (sub1 i)
	      (+ (arithmetic-shift num 8)
		 (u8vector-ref v (sub1 i)))))))

(define (number->u8vector size number)
  (let ((v (make-u8vector size 0)))
    (let loop ((i size)
	       (num number))
      (if (zero? i)
	  (if (zero? num)
	      v
	      ;; Internal error
	      (error (sprintf "Number too large: ~A can't be split into an u8vector of ~A entries" number size)))
	  (begin
	    (u8vector-set! v (- size i) (inexact->exact (modulo num 256))) ; XXX
	    (loop (sub1 i) (quotient num 256)))))))

(define (u8vector-slice v start length)
  (subu8vector v start (+ start length)))

(define (unknown-message-error message-type)
  (signal
   (make-composite-condition
    (make-property-condition 'exn 'message (sprintf "Unknown 9p message type: ~S" message-type))
    (make-property-condition '9p-protocol 'message-type message-type))))

(define (packet-read-error message)
  (signal
   (make-composite-condition
    (make-property-condition 'exn 'message message)
    (make-property-condition '9p-packet-read-error))))

;; Locate the message in the list along with its numerical code
(define (find-message message-type)
  (let loop ((msgs message-types)
	     (pos  0))
    (cond
     ((null? msgs) (unknown-message-error message-type))
     ((eq? (caar msgs) message-type) (values (car msgs) (+ 100 pos)))
     (else (loop (cdr msgs) (add1 pos))))))

(define (send-packet port packet)
  (map (lambda (v) (write-u8vector v port)) packet)
  (flush-output port))

(define (count-packet-size packet)
  (fold (lambda (v total) (+ total (u8vector-length v))) 0 packet))

;; Total size of all u8vectors in this packet (as a u8vector)
(define (packet-size len packet)
  (number->u8vector len (+ len (count-packet-size packet))))

(define (stat-size len packet)
  (number->u8vector len (count-packet-size packet)))

;; Create a 'message format error' condition.
;; This condition signals a protocol violation
(define (message-format-error message-type expected actual . rest)
  (let-optionals rest ((information #f))
    (signal
     (make-composite-condition
      (if information
	  (make-property-condition 'exn 'message (sprintf "~A: Expected an argument of the form ~S, got: ~S" information expected actual))
	  (make-property-condition 'exn 'message (sprintf "Expected an argument of the form ~S, got: ~S" expected actual)))
      (make-property-condition '9p-protocol 'message-type message-type 'expected expected 'actual actual)))))

;; Pack an argument for network transfer and check it against the template type.
;; Return value is a list of u8vectors that encode this argument.
(define (pack-argument message-type type arg)
  (if (and (list? type) (null? (cdr type)))  ; If cdr isn't null, it's malformed
      (begin
        ;; This code is rather ugly and over-specialized.  It's only necessary
        ;; for the Twalk/Rwalk message types because only those need list types
	(if (list? arg)
	    (let ((result (apply append (map (lambda (entry) (pack-argument message-type (car type) entry)) arg)))
                  (item-size (case (car type) ((string) 2) ((qid) 3))))
	      (cons (number->u8vector 2 (/ (length result) item-size)) result))
	    (message-format-error message-type type arg)))
      (case type
	((access-mode)
	 (list (number->u8vector 1 arg)))
	((tag)
	 (list (number->u8vector 2 arg)))
	((msize fid time permission-mode datasize)
	 (list (number->u8vector 4 arg)))
	((qid)
	 (list (number->u8vector 1 (qid-type arg))
	       (number->u8vector 4 (qid-version arg))
	       (number->u8vector 8 (qid-path arg))))
	((filesize) (list (number->u8vector  8 arg)))
	((data)
	 (list (number->u8vector 4 (u8vector-length arg)) arg))
	((string)
	 (list (number->u8vector 2 (string-length arg)) (blob->u8vector/shared (string->blob arg))))
	;; Internal error
	(else (error (sprintf "Unknown type: ~S, arg = ~S" type arg))))))

(define (unparse-packet message-type orig-template orig-contents)
  (let loop ((template orig-template)
	     (contents orig-contents)
	     (data (list)))
    (cond
     ((null? template)
      (if (null? contents)
          data
	  (message-format-error message-type orig-template orig-contents "Too many arguments for message")))
     ((null? contents)
      (message-format-error message-type orig-template orig-contents "Too few arguments for message"))
     ((eq? (car template) 'statsize)  ;; Ugly exception. Continue with new list
      (let* ((rest (loop (cdr template) contents (list))))
	(append data (cons (stat-size 2 rest) rest))))
     ((eq? (car template) 'dev)  ;; "kernel use"
      (loop (cdr template) contents (append data (list (number->u8vector 4 0)))))
     ((eq? (car template) 'type)  ;; "kernel use"
      (loop (cdr template) contents (append data (list (number->u8vector 2 0)))))
     (else
      (loop (cdr template)
	    (cdr contents)
	    (append data (pack-argument message-type
					(car template)
					(car contents))))))))

(define (construct-packet code message-type tag contents)
  (let* ((template (cdr message-type))
         (body (unparse-packet (car message-type) template contents))
         (data (append (list (u8vector code) (number->u8vector 2 tag)) body))
         (size (packet-size 4 data)))
    (cons size data)))

(define (send-message outport message)
  (receive (template code)
      (find-message (message-type message))
    (send-packet outport
		 (construct-packet code
				   template
				   (if (eq? (message-type message) 'Tversion)
				       notag
				       (message-tag message))
				   (message-contents message)))))

(define-record message type tag contents)

(define (read-packet port)
  (let ((length-header (read-u8vector 4 port)))
    (cond
     ;; Zero-length header arises if the connection is closed
     ((zero? (u8vector-length length-header)) #!eof)
     ((< (u8vector-length length-header) 4)
      (packet-read-error "Malformed partial length"))
     (else
      (let ((size (u8vector->number length-header)))
        (if (< size 4)
            (packet-read-error "Illegal packet length of less than four bytes")
            (let ((packet (read-u8vector (- size 4) port)))
              (if (= (u8vector-length packet) (- size 4))
                  packet
                  (packet-read-error "Truncated packet")))))))))

;; Unpack an argument from the network and make something useful out
;; of it (a list of stuff and the length of the stuff parsed)
(define (unpack-argument type packet offset)
  (if (and (list? type) (null? (cdr type))) ; If cdr isn't null, it's malformed
      (let ((todo (u8vector->number (u8vector-slice packet offset 2))))
	(let build-result ((step 0)
			   (len 0)
			   (offset (+ offset 2))
			   (result '()))
	  (if (= step todo)
	      (values (+ 2 len) (list result)) ; + 2 for the 2-byte length of the list
	      (receive (piece-length piece)
		   (unpack-argument (car type) packet offset)
		 (build-result (add1 step)
			       (+ len piece-length)
			       (+ offset piece-length)
			       (append result piece))))))
      (case type
	((access-mode)
	 (values 1 (list (u8vector->number (u8vector-slice packet offset 1)))))
	((tag)
	 (values 2 (list (u8vector->number (u8vector-slice packet offset 2)))))
	((msize fid permission-mode datasize time)
	 (values 4 (list (u8vector->number (u8vector-slice packet offset 4)))))
	((qid)
	 (let ((mode (u8vector->number (u8vector-slice packet offset 1)))
		(version (u8vector->number (u8vector-slice packet (+ offset 1) 4)))
		(path (u8vector->number (u8vector-slice packet (+ offset 5) 8))))
	  (values 13 (list (make-qid mode version path)))))
	((filesize)
	 (values 8 (list (u8vector->number (u8vector-slice packet offset 8)))))
	((data)
	 (let ((datasize (u8vector->number (u8vector-slice packet offset 4))))
	   (values (+ datasize 4) (list (u8vector-slice packet (+ offset 4) datasize)))))
	((statsize type)
	 (values 2 (list))) ; type is "for kernel use", statsize is redundant
	((dev)
	 (values 4 (list))) ; dev is "for kernel use", discard
	((string)
	 (let* ((len (u8vector->number (u8vector-slice packet offset 2)))
		(str (blob->string
		      (u8vector->blob/shared
		       (u8vector-slice packet (+ offset 2) len)))))
	   (values (+ 2 len) (list str))))
	;; Internal error
	(else
	 (error (sprintf "Unknown type: ~A, packet = ~S, offset = ~A"
			 type packet offset))))))

(define (parse-data packet offset length type orig-template)
  (let loop ((offset offset)
             (template orig-template)
             (data '()))
    ;;; XXX The error checking here is iffy
    (cond
     ((null? template)
      (if (or (not length) (= offset length))
          (values offset data)
          (message-format-error type orig-template
                                packet "Too large packet for message")))
     ((and length (= offset length))
      (message-format-error type orig-template
                            packet "Too small packet for message"))
     (else
      (receive (fragment-size contents)
               (unpack-argument (car template) packet offset)
               (loop (+ offset fragment-size)
                     (cdr template)
                     (append data contents)))))))

;; Extract (tag message-type . message-contents) from a packet u8vector
(define (deconstruct-packet packet)
  (let* ((code (u8vector->number (subu8vector packet 0 1)))
	 (message-type (list-ref message-types (- code 100)))
	 (tag (u8vector->number (subu8vector packet 1 3)))
	 (packet-length (u8vector-length packet)))
    (receive (offset packet-contents) (parse-data packet 3 packet-length (car message-type) (cdr message-type))
             (make-message (car message-type) tag packet-contents))))

(define (receive-message inport)
  (let* ((packet (read-packet inport)))
    (if (eof-object? packet)
        #!eof
        (deconstruct-packet packet))))

;; Ugly hack needed because READ is overloaded to return structured
;; data if we're reading a dir
(define (data->full-directory-listing data show-dotfiles?)
  (receive (message-structure num)
	   (find-message 'Rstat)
    (let next-entry ((entries (list))
		     (offset 0))
      (if (= offset (u8vector-length data))
	  entries
          (receive (new-offset entry) (parse-data data offset #f 'Rstat (cddr message-structure))
                   (let ((name (list-ref entry 5)))
                     (if (and (not show-dotfiles?) (char=? (string-ref name 0) #\.))
                         (next-entry entries new-offset)
                         (next-entry (cons entry entries) new-offset))))))))

(define (data->directory-listing data show-dotfiles?)
  (map (lambda (entry) (list-ref entry 5))
       (data->full-directory-listing data show-dotfiles?)))

(define (flatten-u8vector vl)
  (let* ((len (fold (lambda (v acc)
                      (+ acc (u8vector-length v)))
                    0
                    vl))
         (vec (make-u8vector len)))
    (let loop ((offset 0)
               (vl vl))
      (if (null? vl)
          vec
          (begin
           (move-memory! (car vl) vec
                         (u8vector-length (car vl))
                         0 offset)
           (loop (+ offset (u8vector-length (car vl)))
                 (cdr vl)))))))

(define (full-directory-listing->data dir max-length)
  (receive (message-structure num)
           (find-message 'Rstat)
   (let next-entry ((data (list))
                    (dir dir)
                    (bytes-used 0))
     (if (null? dir)
         (values (flatten-u8vector data) '())
         (let ((entry (unparse-packet 'Rstat (cddr message-structure) (car dir))))
           (let ((new-bytes-used (+ bytes-used (count-packet-size entry))))
             (if (> new-bytes-used max-length)
                 ; Size overrun
                 (values (flatten-u8vector data) dir)
                 ; No size overrun, continue!
                 (next-entry
                  (append entry data)
                  (cdr dir)
                  new-bytes-used))))))))

)