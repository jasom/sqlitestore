;;;; sqlitestore.lisp

(in-package #:sqlitestore)

;;; "sqlitestore" goes here. Hacks and glory await!

(defconstant +hash+ 'ironclad:blake2/256)
(defconstant +initial-a+ 1)
(defconstant +initial-b+ 2)
(defconstant +min-length+ 256)
(defconstant +max-length+ (* 1024 1024))


;;(declaim (optimize (speed 3)))
(declaim (inline hash))
(defun hash (seq)
  (ironclad:digest-sequence +hash+ seq))

(declaim (inline fletcher-16 inverse-fletcher-16))
(defun fletcher-16 (a b byte)
  (declare (type (unsigned-byte 8) a b byte))
  (setf
   a (mod (+ a byte) 255)
   b (mod (+ a b) 255))
  (values a b))

(defun inverse-fletcher-16 (a b window byte)
  (declare (type (unsigned-byte 8) a b byte)
	   (type (unsigned-byte 16) window))
  (setf
   a (mod (- a byte) 255)
   b (mod (- b (* byte window) +initial-a+) 255))
  (values a b))

(defun fast-peek-backwards (buffer amount)
  (declare (type fixnum amount))
  (let ((idx (- (fast-io::output-buffer-fill buffer)
		amount)))
    (if (>= idx 0)
	(aref (fast-io::output-buffer-vector buffer) idx) 
	(loop for item in (reverse (fast-io::output-buffer-queue buffer))
	   do (incf idx (length item))
	   unless (< idx 0)
	   return (aref item idx)))))


(defun rolling (input)
  (with-fast-output (buffer)
    (handler-case
	(let ((a +initial-a+)
	      (b +initial-b+))
	  (declare (type (unsigned-byte 8) a b))
	  (loop repeat +min-length+
	     for byte = (fast-read-byte input)
	       do
	       (setf (values a b)
		     (fletcher-16 a b byte))
	       (fast-write-byte byte buffer))

	  (loop
	     for byte = (fast-read-byte input)
	       do
	       (setf (values a b) (inverse-fletcher-16 a b +min-length+
						       (fast-peek-backwards buffer +min-length+))
		     (values a b) (fletcher-16 a b byte))
	       (fast-write-byte byte buffer)
	     until
	       (or (= a b 0)
		   (>= (buffer-position buffer) +max-length+))))
      (end-of-file ()))))


;;; database fns TODO move to different file

(defvar *connection*)

(defun next-version (name)
  (let ((version
	 (sqlite:execute-single *connection*
				     "SELECT MAX(version) FROM files WHERE name=?"
				     name)))
    (if version
	(1+ version)
	1)))

(defun chunk-id (data)
  (let* ((digest (hash data))
	 (idx (sqlite:execute-single
	       *connection*
	       "SELECT id FROM chunks WHERE hash=?"
	       digest)))
    (when idx (return-from chunk-id idx))
    (format *error-output* "Inserting~%")
    (sqlite:execute-non-query *connection*
			      "INSERT INTO chunks (hash, data) VALUES (?, ?)"
			      digest data)
    (sqlite:last-insert-rowid *connection*)))

(defun add-file (name)
  (let ((v (next-version name)))
    (sqlite:execute-non-query *connection*
			      "INSERT INTO files (name, version) VALUES (?, ?)"
			      name v)
    (sqlite:last-insert-rowid *connection*)))

(defun create-tables ()
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE chunks
                             (id INTEGER PRIMARY KEY, hash BLOB UNIQUE, data BLOB)")
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE files
                (id INTEGER PRIMARY KEY, name blob, version INTEGER, size INTEGER)")
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE file_contents
                (id INTEGER, idx, INTEGER, contents INTEGER)"))

(defun file-id (name version)
  (or
   (sqlite:execute-single *connection*
			  "SELECT id from files WHERE name=? AND version=?"
			  name version)
   (error "Could not locate file ~A version ~a" name version)))

(defun add-chunks (file-id input)
  (let ((length 0))
    (loop for index from 0
       for buf = (rolling input)
	 until (zerop (length buf))
	 do
	 (sqlite:execute-single *connection*
				"INSERT INTO file_contents (id, idx, contents) VALUES (?, ?, ?)"
				file-id index (chunk-id buf))
	 (incf length (length buf)))
    (sqlite:execute-non-query *connection*
			      "UPDATE files SET size=? WHERE id=?"
			      length file-id)))

(defun cat-file (file-id output)
  (let ((length (sqlite:execute-single *connection*
				       "SELECT size FROM files where id=?"
				       file-id)))
    (loop
       for index from 0
       while (< sofar length)
       for chunk-id =
	 (sqlite:execute-single
	  "SELECT contents FROM file_contents WHERE id=? AND idx=?"
	  file-id index)
       for chunk = (sqlite:execute-single *connection*
					  "SELECT data FROM chunks WHERE id=?"
					  chunk-id)
       unless chunk do (Error "Error fetching chunk: ~A" chunk-id)
       do (fast-write-sequence chunk output)
       sum (length chunk) into sofar)))

(defun stdin-binary ()
  #+sbcl(sb-sys:make-fd-stream 0 :input t
			       :element-type '(unsigned-byte 8))
  #-sbcl(error "Implement stdin-binary for your implementation"))

(defun stdout-binary ()
  #+sbcl(sb-sys:make-fd-stream 0 :output t
			       :element-type '(unsigned-byte 8))
  #-sbcl(error "Implement stdin-binary for your implementation"))
			       
(defun create-command (dbpath)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (sqlite:with-open-database (*connection* dbpath)
      (create-tables))))

(defun add-command (dbpath filename)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (sqlite:with-open-database (*connection* dbpath)
      (fast-io:with-fast-input (stdin nil (stdin-binary))
	(add-chunks
	 (add-file filename)
	 stdin)))))

(defun restore-command (dbpath filename &optional version)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (sqlite:with-open-database (*connection* dbpath)
	(let ((version
	       (if version
		   (parse-integer version)
		   (next-version filename))))
	  (fast-io:with-fast-output (stdout (stdout-binary))
	    (cat-file (file-id filename version)
		      stdout))))))

(defun main ()
  (case (make-keyword (string-upcase (car (uiop:command-line-arguments))))
    (:add (apply #'add-command (cdr (uiop:command-line-arguments))))
    (:create (apply #'create-command (cdr (uiop:command-line-arguments))))
    (:restore (apply #'restore-command (cdr (uiop:command-line-arguments))))
    (t (error "Unknow command: ~S" (car (uiop:command-line-arguments))))))
