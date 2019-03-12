;;; 9p-server-vfs.scm
;
;; An implementation of the Plan 9 File Protocol (9p)
;; This egg implements the version known as 9p2000 or Styx.
;;
;; This file contains a higher-level infrastructure on top of
;; 9p-server, implementing a simplistic virtual filesystem.
;;
;; It's not very complete at the moment.
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

(require-library 9p-lolevel 9p-server srfi-1 srfi-4 srfi-69)

(module 9p-server-vfs
        (new-filesystem
         insert-file! insert-static-file! insert-simple-file!
         insert-directory!
         vfs-serve)

(import chicken
        scheme
        9p-lolevel
        9p-server
        srfi-1
        srfi-4
        srfi-69)

(define-record file
  type
  id
  name
  perms
  uname
  gname
  muname
  size-if-known ; if #f, get-contents will be called and the result measured
  atime
  mtime
  get-contents ; Closure that returns a u8vector, blob or string, given the file object and the Open State
  parent-id ; Parent directory ID
  children ; List of child file objects
  handle-open! ; returns a new Open State
  handle-clunk! ; passed an Open State
  )

(define-record-printer (file f out)
  (fprintf out "#<file ~s: ~s>"
           (file-id f)
           (file-name f)))

(define (file-qid file)
  (make-qid (file-type file) 1 (file-id file)))

(define (file-stat file)
  (list (file-qid file)
        (bitwise-ior (file-perms file) (arithmetic-shift (file-type file) 24))
        (file-atime file)
        (file-mtime file)
        (file-size file)
        (file-name file)
        (file-uname file)
        (file-gname file)
        (file-muname file)))

(define (file-contents file user-state)
  (let ((c ((file-get-contents file) file user-state)))
    (cond
     ((u8vector? c) c)
     ((string? c) (blob->u8vector/shared (string->blob c)))
     ((blob? c) (blob->u8vector/shared c))
     (else (error "Invalid file contents" c)))))

(define (file-size file)
  (if (zero? (bitwise-and (file-type file) qtdir))
   (if (file-size-if-known file)
       (file-size-if-known file)
       (u8vector-length (file-contents file #f)))
   0)) ; Directories must report zero size

(define-record filesystem
  files
  file-id-counter
  logging?)

(define-record directory-reader
  last-offset
  remaining-entries)

(define (new-filesystem root-perms root-uname root-gname root-atime root-mtime logging?)
  (let ((fs (make-filesystem (make-hash-table) 1 logging?)))
    (hash-table-set! (filesystem-files fs) 0
                     (make-file
                      qtdir
                      0
                      "/"
                      root-perms
                      root-uname
                      root-gname
                      root-uname
                      #f
                      root-atime
                      root-mtime
                      #f ; Directories never have their contents asked for
                      #f
                      '()
                      (lambda (file) (make-directory-reader 0 (map file-stat (file-children file))))
                      (lambda (file state) (void))))
    fs))


;; FIXME: Add some specialised insert-file!s
;; Eg, in this case, we can have an insert-static-file! that just has a string/blob/u8vector
;; contents set at creation and a known size.
;;
;; And we can have an insert-simple-file! that has a callback called on open that
;; returns the file's contents as the state and reads are handled from that, with
;; the option of returning a hardcoded size or handling a stat as an anonymous
;; open.
;;
;; And we can have support for writable files, such as append-only files
;; that call a callback on each write, single-write files that call a callback
;; on close with the entire written content, etc.

(define (insert-file! filesystem type name perms uname gname muname size-if-known atime mtime get-contents parent-id handle-open! handle-clunk!)
  (let* ((id (filesystem-file-id-counter filesystem))
         (f (make-file type id name perms uname gname muname size-if-known atime mtime get-contents parent-id '() handle-open! handle-clunk!))
         (parent-dir (if parent-id
                         (hash-table-ref (filesystem-files filesystem) parent-id)
                         #f)))
    (filesystem-file-id-counter-set! filesystem (+ id 1))
    (hash-table-set! (filesystem-files filesystem) id f)
    (when parent-dir
          (let* ((old-children (file-children parent-dir))
                 (new-children (cons f old-children)))
            (file-children-set! parent-dir new-children)))
    id))

(define (insert-directory! filesystem name perms uname gname muname atime mtime parent-id)
  (insert-file! filesystem
                qtdir
                name perms uname gname muname #f atime mtime
                #f
                parent-id
                (lambda (file)
                  (make-directory-reader 0 (map file-stat (file-children file))))
                (lambda (file state) (void))))

;; Contents is a constant value
(define (insert-static-file! filesystem name perms uname gname muname atime mtime contents parent-id)
  (insert-file! filesystem
                qtfile
                name perms uname gname muname
                #f
                atime mtime
                (lambda (file state) contents)
                parent-id
                (lambda (file) #f)
                (lambda (file state) (void))))

;; Contents is a thunk called when the file is opened
(define (insert-simple-file! filesystem name perms uname gname muname atime mtime contents parent-id)
  (insert-file! filesystem
                qtfile
                name perms uname gname muname
                #f
                atime mtime
                (lambda (file state) (if state state (contents)))
                parent-id
                (lambda (file) (contents))
                (lambda (file state) (void))))

;; FIXME: Add remove-file!, insert-directory!, remove-directory!, etc.

(define (filesystem-file filesystem id)
  (hash-table-ref (filesystem-files filesystem) id))

(define (filesystem-root filesystem)
  (filesystem-file filesystem 0))

(define (filesystem-walk filesystem parent-dir name)
  (call-with-current-continuation
   (lambda (return)
     (let ((dirlist (file-children parent-dir)))
       (if (list? dirlist)
           (for-each (lambda (file)
                       (when (string=? (file-name file) name)
                             (return file)))
                     dirlist)
           #f) ;; #f not a directory
       #f)))) ;; #f not found

;; 9P2000

(define-record file-open
  file
  contents
  user-state)

(define-record-printer (file-open fo out)
  (fprintf out "#<file-open ~s/~s>"
           (file-open-file fo)
           (file-open-user-state fo)))

(define +block-size+ 16384)

(define (dump-message filesystem Ttype message)
  (when (filesystem-logging? filesystem)
        (printf "~S: ~S\n" Ttype message)))

(define (dump-message-fid filesystem Ttype message fid-value)
  (when (filesystem-logging? filesystem)
        (printf "~S: ~S ~S\n" Ttype message fid-value)))

(define ((handle-version filesystem) message)
  (dump-message filesystem 'Tversion message)
  (min +block-size+ (car message)))

(define ((handle-auth filesystem) message bind-fid! reply! error!)
  (dump-message filesystem 'Tauth message)
  (error! "You don't need to authenticate with me.")
  (void))

(define ((handle-flush filesystem) message reply! error!)
  (dump-message filesystem 'Tflush message)
  (reply! '())
  (void))

(define ((handle-attach filesystem) message auth-fid-value bind-fid! reply! error!)
  (dump-message-fid filesystem 'Tattach message auth-fid-value)
  (let ((root (filesystem-root filesystem)))
   (bind-fid! (make-file-open root #f #f))
   (reply! (list (file-qid root)))))

(define ((handle-walk filesystem) message parent-fid-value bind-fid! reply! error!)
  (dump-message-fid filesystem 'Twalk message parent-fid-value)
  (let loop ((names (caddr message))
             (parent (file-open-file parent-fid-value))
             (qids '()))
    (cond
     ((null? names)
      (bind-fid! (make-file-open parent #f #f))
      (reply! (list (reverse qids))))
     (else
      (let* ((name (car names))
             (child (filesystem-walk filesystem parent name)))
        (if child
            (loop (cdr names)
                  child
                  (cons (file-qid child) qids))
            (begin ;; Nonexistant child, stop here
              (if (null? qids)
                  (error! "Unknown filename")
                  (reply! (list (reverse qids)))))))))))

(define ((handle-open filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Topen message fid-value)
  (let* ((file (file-open-file fid-value))
         (user-state ((file-handle-open! file) file)))
    (file-open-user-state-set! fid-value user-state)
    (if (eq? (file-type file) qtdir)
     (file-open-contents-set! fid-value #f)
     (file-open-contents-set! fid-value (file-contents file user-state)))
    (reply! (list (file-qid file) +block-size+))))

(define ((handle-create filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Tcreate message fid-value)
  (error! "Not yet implemented"))

(define (handle-file-read filesystem message fid-value reply! error!)
  (let* ((file (file-open-file fid-value))
         (contents (file-open-contents fid-value))
         (offset (second message))
         (count (min (- (u8vector-length contents) offset)
                     (third message))))
    (reply! (list
             (subu8vector contents offset (+ offset count))))))

(define (handle-dir-read filesystem message fid-value reply! error!)
  (let* ((file (file-open-file fid-value))
         (reader (file-open-user-state fid-value))
         (previous-offset (directory-reader-last-offset
                           reader))
         (offset (second message))
         (remaining-entries
          (if (zero? offset)
              (map file-stat (file-children file))
              (directory-reader-remaining-entries
               reader)))
         (count (third message)))
    (if ;; Enforce rules about directory reads - they must be sequential
     (not (or
           (= previous-offset offset)
           (= offset 0)))
     (error! "Directory reads must be from the previous offset or back to 0")
     (begin
       ;; Return as many whole stat entries as can be packed into a read
       ;; without leaving any partials
       ;; Each stat entry is prefixed with its length in bytes, so we can
       ;; skip thruogh the chain easily.
       (receive (response new-remaining-entries)
                (full-directory-listing->data
                 remaining-entries count)
                (directory-reader-last-offset-set!
                 reader (+ offset (u8vector-length response)))
                (directory-reader-remaining-entries-set!
                 reader new-remaining-entries)
                (reply! (list response)))))))

(define ((handle-read filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Tread message fid-value)
  (let ((file (file-open-file fid-value)))
    (if (eq? (file-type file)
             qtdir)
        (handle-dir-read filesystem message fid-value reply! error!)
        (handle-file-read filesystem message fid-value reply! error!))))

(define ((handle-write filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Twrite message fid-value)
  (error! "Not yet implemented"))

(define ((handle-clunk filesystem) fid-value reply! error!)
  (dump-message-fid filesystem 'Tclunk '() fid-value)
  (let ((file (file-open-file fid-value))
        (user-state (file-open-user-state fid-value)))
    ((file-handle-clunk! file) file user-state))
  (reply! '()))

(define ((handle-remove filesystem) fid-value reply! error!)
  (dump-message-fid filesystem 'Tremove '() fid-value)
  (error! "Not yet implemented"))

(define ((handle-stat filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Tstat message fid-value)
  (reply! (file-stat (file-open-file fid-value))))

(define ((handle-wstat filesystem) message fid-value reply! error!)
  (dump-message-fid filesystem 'Twstat message fid-value)
  (error! "Not yet implemented"))

(define ((handle-disconnect filesystem))
  (if (filesystem-logging? filesystem) (printf "Disconnected\n")))

(define (vfs-serve in out filesystem)
  (serve in out
         `((version . ,(handle-version filesystem))
           (auth . ,(handle-auth filesystem))
           (flush . ,(handle-flush filesystem))
           (attach . ,(handle-attach filesystem))
           (walk . ,(handle-walk filesystem))
           (open . ,(handle-open filesystem))
           (create . ,(handle-create filesystem))
           (read . ,(handle-read filesystem))
           (write . ,(handle-write filesystem))
           (clunk . ,(handle-clunk filesystem))
           (remove . ,(handle-remove filesystem))
           (stat . ,(handle-stat filesystem))
           (wstat . ,(handle-wstat filesystem))
           (disconnect . ,(handle-disconnect filesystem)))))

)