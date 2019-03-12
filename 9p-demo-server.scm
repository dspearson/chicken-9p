(use 9p-server-vfs 9p-lolevel tcp srfi-18 posix)

;; A static filesystem (for now)

(define filesystem (new-filesystem
                    (+ perm/irusr perm/ixusr perm/irgrp perm/ixgrp perm/iroth perm/ixoth)
                    "0"
                    "0"
                    (current-seconds)
                    (current-seconds) #t))

(insert-static-file! filesystem
                     "hello"
                     (+ perm/irusr perm/irgrp perm/iroth)
                     "0"
                     "0"
                     "0"
                     (current-seconds) (current-seconds)
                     "Hello, World!\n"
                     0)

(define subdir
  (insert-directory! filesystem
                     "dynamic-content"
                     (+ perm/irusr perm/irgrp perm/iroth
                        perm/ixusr perm/ixgrp perm/ixoth)
                     "0"
                     "0"
                     "0"
                     (current-seconds) (current-seconds)
                     0))

(insert-simple-file! filesystem
                     "test"
                     (+ perm/irusr perm/irgrp perm/iroth)
                     "0"
                     "0"
                     "0"
                     (current-seconds) (current-seconds)
                     (lambda ()
                       (string-append (seconds->string) "\n"))
                     subdir)

(parameterize ((tcp-read-timeout #f)
               (tcp-buffer-size 65536))
 (let ((listener (tcp-listen 1564)))
   (let accept-loop ()
     (receive (in out) (tcp-accept listener)
              (thread-start! (make-thread
                              (lambda ()
                                (vfs-serve in out filesystem)
                                (close-input-port in)
                                (close-output-port out)))))
     (accept-loop))))