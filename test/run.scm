(use test)
(use 9p-lolevel)
(use 9p-server)
(use 9p-client)
(use utf8)

(test-group "Message encoding and decoding"
 (define (message->list message)
   (list (message-type message)
         (message-tag message)
         (message-contents message)))

 (define (list->message lmessage)
   (make-message (first lmessage)
                 (second lmessage)
                 (third lmessage)))

 (define (encode-message message)
   (with-output-to-string
     (lambda ()
       (send-message (current-output-port) message))))

 (define (decode-message string)
   (with-input-from-string string
     (lambda ()
       (receive-message (current-input-port)))))

 (define (check-message-encoding name message)
   (test name
         message
         (let ((message-encoded (encode-message (list->message message))))
           (message->list (decode-message message-encoded)))))

 (check-message-encoding "Tversion"
                         '(Tversion 65535 (123 "Hello")))

 (check-message-encoding "Rversion"
                         '(Rversion 1 (123 "Hello")))

 (check-message-encoding "Tauth"
                         '(Tauth 2 (123 "Hello" "World")))

 (check-message-encoding "Rauth"
                         `(Rauth 3 (,(make-qid 1 2 3))))

 (check-message-encoding "Tattach"
                         '(Tattach 4 (123 456 "Hello" "World")))

 (check-message-encoding "Rattach"
                         `(Rattach 5 (,(make-qid 1 2 3))))

 (check-message-encoding "Terror"
                         '(Terror 6 ()))

 (check-message-encoding "Rerror"
                         '(Rerror 7 ("Hello")))

 (check-message-encoding "Tflush"
                         '(Tflush 8 (123)))

 (check-message-encoding "Rflush"
                         '(Rflush 9 ()))

 (check-message-encoding "Twalk"
                         '(Twalk 10 (123 456 ("Hello" "World"))))

 (check-message-encoding "Rwalk"
                         `(Rwalk 11 ((,(make-qid 1 2 3) ,(make-qid 4 5 6)))))

 (check-message-encoding "Topen"
                         '(Topen 12 (123 123)))

 (check-message-encoding "Ropen"
                         `(Ropen 13 (,(make-qid 1 2 3) 123)))

 (check-message-encoding "Tcreate"
                         '(Tcreate 14 (123 "Hello" 123 123)))

 (check-message-encoding "Rcreate"
                         `(Rcreate 15 (,(make-qid 1 2 3) 123)))

 (check-message-encoding "Tread"
                         '(Tread 16 (123 456 789)))

 (check-message-encoding "Rread"
                         `(Rread 17 (,(u8vector 1 2 3 4 5 6))))

 (check-message-encoding "Twrite"
                         `(Twrite 18 (123 456 ,(u8vector 1 2 3 4 5 6))))

 (check-message-encoding "Rwrite"
                         '(Rwrite 19 (123)))

 (check-message-encoding "Tclunk"
                         '(Tclunk 20 (123)))

 (check-message-encoding "Rclunk"
                         '(Rclunk 21 ()))

 (check-message-encoding "Tremove"
                         '(Tremove 22 (123)))

 (check-message-encoding "Rremove"
                         '(Rremove 23 ()))

 (check-message-encoding "Tstat"
                         '(Tstat 24 (123)))

 (check-message-encoding "Rstat"
                         `(Rstat 25 (,(make-qid 4 5 6) 7 8 9 10 "Hello" "World" "Foo" "Bar")))

 (check-message-encoding "Twstat"
                         `(Twstat 26 (123 ,(make-qid 4 5 6) 7 8 9 10 "Hello" "World" "Foo" "Bar")))

 (check-message-encoding "Rwstat"
                         '(Rwstat 27 ())))

(test-group "Directory encoding"
 (let* ((dirent1 `(,(make-qid 1 2 3) 1 2 3 4 "a1" "b1" "c1" "d1"))
        (dirent2 `(,(make-qid 4 5 6) 1 2 3 4 "." "b2" "c2" "d2"))
        (dir0 (list))
        (dir1 (list dirent1))
        (dir2 (list dirent1 dirent2)))

   (test "Empty" dir0
         (data->full-directory-listing
          (full-directory-listing->data dir0 999999) #t))

   (test "Single entry" dir1
         (data->full-directory-listing
          (full-directory-listing->data dir1 999999) #t))

   (test "Double entry" dir2
         (data->full-directory-listing
          (full-directory-listing->data dir2 999999) #t))

   (test "Skip dotfiles" dir1
         (data->full-directory-listing
          (full-directory-listing->data dir2 999999) #f))

   (test "Truncated encoding (0)" dir0
         (data->full-directory-listing
          (full-directory-listing->data dir2 0) #t))

   (test "Truncated encoding (1)" dir0
         (data->full-directory-listing
          (full-directory-listing->data dir2 1) #t))

   (test "Truncated encoding (56)" dir0
         (data->full-directory-listing
          (full-directory-listing->data dir2 56) #t))

   ; dirent1 takes 57 bytes

   (test "Truncated encoding (57)" dir1
         (data->full-directory-listing
          (full-directory-listing->data dir2 57) #t))

   (test "Truncated encoding (58)" dir1
         (data->full-directory-listing
          (full-directory-listing->data dir2 58) #t))
   
   ; dirent1+dirent2 takes 113 bytes

   (test "Truncated encoding (112)" dir1
         (data->full-directory-listing
          (full-directory-listing->data dir2 112) #t))

   (test "Truncated encoding (113)" dir2
         (data->full-directory-listing
          (full-directory-listing->data dir2 113) #t))

   (test "Truncated encoding (114)" dir2
         (data->full-directory-listing
          (full-directory-listing->data dir2 114) #t))))

(test-group "Client and Server"
  ; FIXME:

  ; Write a test suite that runs a client and a server
  ; and tests one against the other.
  (void)
)
