;;;; zstd.asd

(asdf:defsystem #:zstd
  :description "Describe zstd here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:cffi
               #:alexandria)
  :serial t
  :components ((:file "package")
               (:file "zstd")))

