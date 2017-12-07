;;;; sqlitestore.asd

(asdf:defsystem #:sqlitestore
  :description "Describe sqlitestore here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on ("sqlite"
	       #+sbcl"sb-sprof"
               "ironclad"
	       "alexandria"
	       "lparallel"
	       "zstd"
               "fast-io")
  :serial t
  :components ((:file "package")
               (:file "sqlitestore")))

