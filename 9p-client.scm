;;;; 9p.scm
;
;; An implementation of the Plan 9 File Protocol (9p)
;; This egg implements the version known as 9p2000 or Styx.
;;
;; This file contains a posix-like higher-level client-side
;; abstraction over the lower-level connection and message
;; packing/unpacking provided by 9p-lolevel.
;
; Copyright (c) 2007, 2008, 2012 Peter Bex
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
; ticket tracking system (assign tickets to user 'sjamaan'):
; http://trac.callcc.org
;
; TODO: 9p:rename-file.  Unfortunately, 9p has no native 'move'
; command, so this will be hard and very error-prone to do, at least
; in the case of directories.  Something for another time :)
;
; TODO: Wstat interface (9p:change-file-owner, 9p:change-file-mode)
; Using this interface we can implement a partially correct rename-file: the
; filename can be changed, but not the rest of the path.
; Beware: the "spec" says changing the owner is illegal, except when
; the fs was configed to allow it?  File length can be changed too...
;
; TODO: Think about how to expose non-POSIX things from 9p properly

(require-library posix srfi-1 srfi-4 srfi-13 srfi-18 iset 9p-lolevel)

(module 9p-client
 (connection? request client-connect client-disconnect
  handle? alloc-handle release-handle normalize-path
  path-walk file-open file-close with-handle-to
  file-exists? file-create file-read file-write
  file-stat file-permissions file-access-time
  file-modification-time file-size file-owner file-group
  file-last-modified-by directory? regular-file?
  set-file-position! file-position directory create-directory
  delete-file open-output-file call-with-output-file
  with-output-to-file open-input-file call-with-input-file
  with-input-from-file
  connection-message-size connection-inport connection-outport
  handle-connection handle-fid handle-position handle-iounit
  handle-position-set! handle-iounit-set!
  alloc-handle release-handle)

 (import (except scheme open-output-file call-with-output-file
		 open-input-file call-with-input-file with-input-from-file
		 with-output-to-file)
	 (except chicken delete-file file-exists?) extras lolevel
	 data-structures utils
	 (only ports make-input-port make-output-port)
	 (prefix posix posix:)
	 (only files pathname-strip-directory pathname-directory)
	 srfi-1 srfi-4 srfi-13 srfi-18 iset (prefix 9p-lolevel 9p:))
 
(define-record connection
  inport outport message-size open-fids tags messages
  read-mutex write-mutex record-mutex)

(define (semaphore-lock! mutex)
  (if (eq? (mutex-state mutex) (current-thread))
      (mutex-specific-set! mutex (add1 (mutex-specific mutex)))
      (begin
	(mutex-lock! mutex)
	(mutex-specific-set! mutex 0))))

(define (semaphore-unlock! mutex)
  (cond
   ((not (eq? (mutex-state mutex) (current-thread)))
    (error "The current thread does not own the mutex!"))
   ((> (mutex-specific mutex) 0)
    (mutex-specific-set! mutex (sub1 (mutex-specific mutex))))
   (else (mutex-unlock! mutex))))

(define (with-semaphore mutex thunk)
  (dynamic-wind
      (lambda () (semaphore-lock! mutex))
      thunk
      (lambda () (semaphore-unlock! mutex))))

(define (server-error message-type error-message)
  (signal
   (make-composite-condition
    (make-property-condition
     'exn 'message
     (sprintf "9p server returned ~S for message ~A"
	      error-message message-type))
    (make-property-condition '9p-server-error 'message-type message-type))))

(define (response-error message-type response-type)
  (signal
   (make-composite-condition
    (make-property-condition
     'exn 'message
     (sprintf "9p server returned unexpected response type ~S for message ~A"
	      response-type message-type))
    (make-property-condition '9p-response-error 'message-type message-type))))

;; Allocate the lowest tag that's not in use yet and return it
(define (alloc-tag con)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
     (receive (new-tag tags) (alloc-bit! (connection-tags con) 9p:notag)
       (connection-tags-set! con tags)
       new-tag))))

(define (release-tag con tag)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
     (connection-tags-set! con (bit-vector-set! (connection-tags con) tag #f))))
  (void))

;; Threadsafe send-message.  Returns tag associated with the message,
;; which can be used when reading it.
(define (client-send-message con type args)
  (let ((tag (if (eq? type 'Tversion)
		 9p:notag
		 (alloc-tag con))))
    (with-semaphore
     (connection-write-mutex con)
     (lambda ()
       (9p:send-message (connection-outport con)
			(9p:make-message type tag args))))
    tag))

;; Retrieve a message from the list, if it exists
(define (retrieve-message con tag)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
     (and-let* ((res (find (lambda (x)
			     (= (9p:message-tag x) tag))
			   (connection-messages con))))
       (connection-messages-set! con (remove! (lambda (x)
						(= (9p:message-tag x) tag))
					      (connection-messages con)))
       res))))

;; Store a message in the list
(define (store-message con message)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
     (connection-messages-set! con (cons message (connection-messages con))))))

;; Threadsafe receive-message.
(define (client-receive-message tag con)
  (semaphore-lock! (connection-read-mutex con))
  (let ((message (or
		  (retrieve-message con tag)
		  (9p:receive-message
		   (connection-inport con)))))
    (if (= (9p:message-tag message) tag)
	(begin
	  (unless (= tag 9p:notag) (release-tag con (9p:message-tag message)))
	  (semaphore-unlock! (connection-read-mutex con))
	  message)
	(begin
	  (store-message con message)
	  (semaphore-unlock! (connection-read-mutex con))
	  (client-receive-message tag con)))))

;; Client request.  Sends a message of the given type and args and waits
;; for a matching response (a Rxyz response matches a Txyz request).
(define (request con type . args)
  (let* ((tag (client-send-message con type args))
	 (response (client-receive-message tag con))
	 (expected-type (string->symbol
			 (string-replace (symbol->string type) "R" 0 1))))
    (cond
     ((eq? (9p:message-type response) expected-type)
      response)
     ((eq? (9p:message-type response) 'Rerror)
      (server-error type (car (9p:message-contents response))))
     (else
      (response-error type (9p:message-type response))))))

;; Initialize a connection to a 9p server ("mount"/"bind")
;; Authentication is currently not supported
(define (client-connect inport outport . rest)
  (let-optionals rest ((user "")
		       (mountpoint ""))
    (let* ((fids (make-bit-vector 8))  ; Start with 8 bits. Grow only when needed
	   (tags (make-bit-vector 8))  ; Same for the tags
	   (con (make-connection inport outport #xffffffff
				 fids tags (list)
				 (make-mutex) (make-mutex) (make-mutex)))
	    ; We can handle message size #xffffffff but wmii/libixp
	    ; crashes on that. #x7ffffff is the absolute max for it
	   (answer (request con 'Tversion #x7fffffff "9P2000")))
      (cond
       ((not (string=? "9P2000" (cadr (9p:message-contents answer))))
	(error (sprintf "Incompatible protocol version: ~S"
			(9p:message-contents answer))))
       (else
	(connection-message-size-set! con (car (9p:message-contents answer)))
	;; To authenticate, do a Tauth request, authenticate and use
	;; the fid we got when authenticating instead of nofid below.
	;;
	;; Allocate the root fid using alloc-handle so we
	;; automatically get a finalizer set
	(request con 'Tattach (handle-fid (alloc-handle con))
		 9p:nofid user mountpoint)
	con)))))

;; Sever connection, clunk all fids
(define (client-disconnect con)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
    (let* ((fids (connection-open-fids con)))
      (let loop ((fid (bit-vector-length fids)))
	(when (not (zero? fid))
	      (when (bit-vector-ref fids (sub1 fid))
		    (file-close (make-handle con (sub1 fid) 0 #f)))
	      (loop (sub1 fid))))
      (connection-inport-set! con #f)
      (connection-outport-set! con #f)
      (connection-message-size-set! con #f)
      (connection-open-fids-set! con #f)
      (connection-tags-set! con #f)
      (connection-messages-set! con #f)
      (connection-read-mutex-set! con #f)
      (connection-write-mutex-set! con #f)
      (connection-record-mutex-set! con #f)
      (void)))))

;; File IDs and handles
(define-record handle
  connection fid position iounit)

(define (initialize-iounit! h iounit)
  (handle-iounit-set!
   h
   (if (zero? iounit)
       ;; 23 is the biggest size of a message (write), but libixp uses
       ;; 24, so we do too to stay safe
       (- (connection-message-size (handle-connection h)) 24)
       iounit)))

(define (alloc-bit! bit-vector max)
  (let loop ((highest 0)
	     (size (bit-vector-length bit-vector)))
    (cond
     ((bit-vector-full? bit-vector (add1 highest))
      (loop (add1 highest) size))
     ((>= highest max)
      (error "Cannot allocate new bit for bit-vector"))
     (else
      (values highest (bit-vector-set! bit-vector highest #t))))))

;; Allocate the lowest fid that's not in use yet and return a handle to it
(define (alloc-handle con)
  (with-semaphore
   (connection-record-mutex con)
   (lambda ()
     (receive (new-fid fids) (alloc-bit! (connection-open-fids con) 9p:nofid)
       (connection-open-fids-set! con fids)
       (make-handle con new-fid 0 #f)))))

;; Deallocate the given handle from the list (does _not_ clunk it)
(define (release-handle h)
  (let ((con (handle-connection h)))
   (with-semaphore
    (connection-record-mutex con)
    (lambda ()
      (connection-open-fids-set! con (bit-vector-set! (connection-open-fids con)
						      (handle-fid h)
						      #f))))
   ;; Invalidate the handle
   (handle-connection-set! h #f)
   (handle-fid-set! h #f)
   (handle-iounit-set! h #f)
   (void)))

;; Make a list of path components.  Accepts either a string which it will split
;; at slashes, or a pre-made path component list.
(define (normalize-path path)
  (if (pair? path)
      path
      (string-split path "/")))

;; Obtain a new fid
(define (path-walk con path . rest)
  (let-optionals rest ((starting-point #f))
    (let ((new-handle (alloc-handle con)))
      (handle-exceptions exn (begin (release-handle new-handle) (signal exn))
	(request con 'Twalk (if starting-point (handle-fid starting-point) 0)
		 (handle-fid new-handle) (normalize-path path))
	new-handle))))

(define (file-open con name mode)
  (let ((h (path-walk con name)))
    (handle-exceptions exn (begin (file-close h) (signal exn))
     (let* ((response (request con 'Topen (handle-fid h) mode))
	    (iounit (second (9p:message-contents response))))
       (initialize-iounit! h iounit)
       h))))

;; Clunk a fid
(define (file-close h)
  (when (handle-connection h) ; Ignore if invalid (closing closed handle is ok)
   (request (handle-connection h) 'Tclunk (handle-fid h))
   (release-handle h)))

;; With a temporary handle to a file, perform some other procedure
;; The handle gets walked to and clunked automatically
(define (with-handle-to con path procedure)
  (let ((h (path-walk con path)))
    (handle-exceptions
     exn
     (begin
       (file-close h)
       (signal exn)) ;; Just reraise it
     (let ((result (call-with-values (lambda () (procedure h)) list)))
       (file-close h)
       (apply values result)))))

;; This is a hack, as the server might return other errors beside
;; "file does not exist", but there's no way we can really ask this
;; question otherwise.
(define (file-exists? con path)
  (condition-case (with-handle-to con path (constantly #t))
    ((exn 9p-server-error) #f)))

;; This duplicates much of with-handle-to, but 9p isn't very
;; consistent here: the fid that initially represents the directory is
;; now reused and represents the newly created file, so we can't use
;; with-handle-to (or we'd have to reopen the file after creating,
;; which is not possible in case of tempfiles)
(define (file-create con name perm mode)
  (let ((h (path-walk con (pathname-directory name))))
    (handle-exceptions
     exn
     (begin
       (file-close h)
       (signal exn)) ;; Just reraise it
     (let* ((response (request con 'Tcreate (handle-fid h)
			       (pathname-strip-directory name) perm mode))
	    (iounit (second (9p:message-contents response))))
       (initialize-iounit! h iounit)
       h))))

(define (create-directory con name perm)
  (file-close (file-create con name (bitwise-ior perm 9p:dmdir) 9p:open/rdonly)))

(define (u8vector-append! . vectors)
  (let* ((length (apply + (map u8vector-length vectors)))
	 (result (make-u8vector length)))
    (let next-vector ((vectors vectors)
		      (result-pos 0))
      (if (null? vectors)
	  result
	  (let next-pos ((vector-pos 0)
			 (result-pos result-pos))
	    (if (= vector-pos (u8vector-length (car vectors)))
		(next-vector (cdr vectors) result-pos)
		(begin
		  (u8vector-set! result result-pos
				 (u8vector-ref (car vectors) vector-pos))
		  (next-pos (add1 vector-pos) (add1 result-pos)))))))))

(define (u8vector-slice v start length)
  (subu8vector v start (+ start length)))

;; TODO: Find a way to use an optional buffer to write in, so we don't end up
;; copying a whole lot of data around (overhead!)  -- file-write also has this
;; This is doubly bad because if we have a small message size the copying and
;; appending really becomes a whole lot of overhead.
(define (file-read h size)
  (let loop ((bytes-left size)
	     (total 0)
	     (result (list)))
    (if (zero? bytes-left)
	(list (blob->string
	       (u8vector->blob/shared
		(apply u8vector-append! (reverse result))))
	      total) ; file-read also returns a list of data + length
	(let* ((pos (handle-position h))
	       (receive-size (min bytes-left (handle-iounit h)))
	       (response (request (handle-connection h)
				  'Tread (handle-fid h) pos receive-size))
	       (data (car (9p:message-contents response)))
	       (read (u8vector-length data)))
	  (cond
	   ((< read bytes-left)
	    (handle-position-set! h (+ pos read))
	    ;; Pad with empty u8vector, just like file-read
	    (loop 0 (+ total read)
		  (cons (make-u8vector bytes-left (char->integer #\space))
			(cons data result))))
	   ;; Sometimes the server returns more than we asked for!
	   ;; (when accidentally reading a dir, for example)
	   ((> read bytes-left)
	    (handle-position-set! h (+ pos bytes-left))
	    (loop 0 (+ total bytes-left)
		  (cons (u8vector-slice data 0 bytes-left)
			result)))
	   (else
	    (handle-position-set! h (+ pos read))
	    (loop (- bytes-left read) (+ total read) (cons data result))))))))

(define (file-write h buffer . rest)
  (let ((buffer (if (string? buffer)
		    (blob->u8vector/shared (string->blob buffer))
		    buffer)))
    (let-optionals rest ((size (u8vector-length buffer)))
      (let loop ((bytes-left size)
		 (total 0))
	(if (zero? bytes-left)
	    total
	    (let* ((pos (handle-position h))
		   (send-size (min bytes-left (handle-iounit h)))
		   (response (request (handle-connection h)
				      'Twrite (handle-fid h) pos
				      (u8vector-slice buffer total send-size)))
		   (written (car (9p:message-contents response))))
	      (handle-position-set! h (+ pos written))
	      (if (not (= written send-size))
		  (server-error 'Twrite (sprintf "Unexpected bytecount ~A instead of ~A in Rwrite response (not a proper server error message)" written send-size)))
	      (loop (- bytes-left written) (+ total written))))))))

; (qid permission-mode time time filesize string string string string)
(define (handle-stat h)
  (apply vector (9p:message-contents
		 (request (handle-connection h) 'Tstat (handle-fid h)))))

(define (file-stat con file)
  (with-handle-to
   con file handle-stat))

(define (file-permissions con file)
  (vector-ref (file-stat con file) 1))

(define (file-access-time con file)
  (vector-ref (file-stat con file) 2))

(define (file-modification-time con file)
  (vector-ref (file-stat con file) 3))

;; There is no file-change-time because the protocol does not provide it.

(define (file-size con file)
  (vector-ref (file-stat con file) 4))

;; 5 is file-name, which is rather silly

;;; Important: The following three procedures return _strings_, not IDs
(define (file-owner con file)
  (vector-ref (file-stat con file) 6))

(define (file-group con file)
  (vector-ref (file-stat con file) 7))

(define (file-last-modified-by con file)
  (vector-ref (file-stat con file) 8))

(define (directory? con path)
  (let ((new-handle (alloc-handle con)))
    (handle-exceptions exn (begin (release-handle new-handle) (signal exn))
     (let* ((response (request con 'Twalk 0
			       (handle-fid new-handle)
			       (normalize-path path)))
	    (is-dir (not
		     (zero?
		      (bitwise-and 9p:qtdir
				   (9p:qid-type
				    (last (9p:message-contents response))))))))
       (file-close new-handle)
       is-dir))))

(define (regular-file? con file)
  (not (directory? con file)))

(define (set-file-position! h pos . rest)
  (let-optionals rest ((whence posix:seek/set))
    (cond
     ((< 0 pos) (signal (make-composite-condition
			 (make-property-condition
			  'exn 'message
			  (sprintf "Invalid negative seek position: ~S" pos))
			 (make-property-condition 'bounds))))
     ((eq? whence posix:seek/set)
      (handle-position-set! h pos))
     ((eq? whence posix:seek/cur)
      (handle-position-set! h (+ (handle-position h) pos)))
     ((eq? whence posix:seek/end)
      (let ((size (vector-ref (handle-stat h) 4)))
	((handle-position-set! h (+ size pos)))))
     (else
      (error (sprintf "Unknown seek position type (WHENCE value): ~S"
		      whence))))))

(define file-position handle-position)

(define (read-directory h show-dotfiles?)
  (let loop ((result (list))
	     (pos 0))
    (let* ((response (request (handle-connection h)
			      'Tread (handle-fid h) pos (handle-iounit h)))
	   (data (car (9p:message-contents response)))
	   (read (u8vector-length data)))
      (if (zero? read)
	  (9p:data->directory-listing
	   (apply u8vector-append! (reverse result))
	   show-dotfiles?)
	  (loop (cons data result) (+ pos read))))))

(define (directory con file . rest)
  (let-optionals rest ((show-dotfiles? #f))
   (with-handle-to
    con file
    (lambda (h)
      (if (zero? (bitwise-and 9p:dmdir (vector-ref (handle-stat h) 1)))
	  (signal (make-composite-condition
		   (make-property-condition
		    'exn 'message
		    (sprintf "~S is not a directory!" file))
		   (make-property-condition 'file)))
	  (let* ((response (request con 'Topen (handle-fid h) 9p:open/rdonly))
		 (iounit (second (9p:message-contents response))))
	    (initialize-iounit! h iounit)
	    (read-directory h show-dotfiles?)))))))

(define (delete-file con path)
  (let ((h (path-walk con path)))
    (handle-exceptions exn (begin (release-handle h) (signal exn))
      (request con 'Tremove (handle-fid h)) (release-handle h))))

(define (open-output-file con file . rest)
  (let ((h (if (file-exists? con file)
	       (file-open con file (bitwise-ior 9p:open/wronly 9p:open/trunc))
	       (let-optionals rest ((perm (bitwise-ior 9p:perm/irusr
						       9p:perm/iwusr
						       9p:perm/irgrp
						       9p:perm/iwgrp
						       9p:perm/iroth
						       9p:perm/iwoth)))
			      (file-create con file perm 9p:open/wronly)))))
    (make-output-port (lambda (s) (file-write h s) (void))
		      (lambda () (file-close h)))))

(define (call-with-output-file con file procedure)
  (let ((p (open-output-file con file)))
    (handle-exceptions exn (begin (close-output-port p) (signal exn))
      (let ((result (call-with-values (lambda () (procedure p)) list)))
	(close-output-port p)
	(apply values result)))))

(define (with-output-to-file con file thunk)
  (call-with-output-file con file (lambda (p) (parameterize ((current-output-port p)) (thunk)))))

(define (open-input-file con file)
  (let* ((h (file-open con file 9p:open/rdonly))
	 (buffer #f)
	 (buffer-offset 0)
	 (buffer-size 0))
    (make-input-port (lambda ()
		       ;; This procedure does some
		       ;; string/blob/u8vector gymnastics so it
		       ;; returns raw byte characters both when utf8
		       ;; is loaded and when it's not.  The highlevel
		       ;; "read" procedures are overridden by utf8,
		       ;; but low-level procedures are still expected
		       ;; to return byte-chars.  That's why we can't
		       ;; use string-ref here (because it may really
		       ;; be utf8's string-ref).
		       (if buffer
			   (let ((char (integer->char
					(u8vector-ref buffer buffer-offset))))
			    (set! buffer-offset (add1 buffer-offset))
			    (when (= buffer-offset buffer-size)
				  (set! buffer-offset 0)
				  (set! buffer #f))
			    char)
			   (let ((result (file-read h (min 1024
							   (handle-iounit h)))))
			     (cond
			      ((zero? (second result)) #!eof)
			      ((= (second result) 1)
			       (integer->char
				(u8vector-ref
				 (blob->u8vector/shared
				  (string->blob (car result)))
				 0)))
			      (else (set! buffer
					  (blob->u8vector/shared
					   (string->blob (car result))))
				    (set! buffer-size (second result))
				    (set! buffer-offset 1)
				    (integer->char (u8vector-ref buffer 0)))))))
		     (constantly #t)
		     (lambda ()
		       (file-close h)))))

(define (call-with-input-file con file procedure)
  (let* ((p (open-input-file con file))
	 (result (call-with-values (lambda () (procedure p)) list)))
    (close-input-port p)
    (apply values result)))

(define (with-input-from-file con file thunk)
  (call-with-input-file con file
			(lambda (p)
			  (parameterize ((current-input-port p)) (thunk)))))
)
