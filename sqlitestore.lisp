;;;; sqlitestore.lisp
(in-package #:sqlitestore)

;;; "sqlitestore" goes here. Hacks and glory await!

(defconstant +hash+ 'ironclad:blake2/256)
(defconstant +initial-a+ 1)
(defconstant +initial-b+ 2)
(defconstant +min-length+ 16)
(defconstant +max-length+ (* 1024 1024))

(defun compress (x) (zstd:compress x 6))
(defun decompress (x) (zstd::decompress x +max-length+))
;;(defun compress (x) x)
;;(defun decompress (x) x)

(declaim (optimize (debug 0) (speed 3)))
;(declaim (optimize (debug 3)))
#.(defconstant +buffer-size+ #x8000)
(defconstant +buffer-size+ #x8000)

(defstruct buffered-reader
  (buffer (fast-io:make-octet-vector +buffer-size+)
	  :type (simple-array octet (#.+buffer-size+)))
  (pos 0 :type fixnum)
  (fill 0 :type fixnum)
  (stream nil :type stream))

(declaim (inline buffered-reader-fill-buffer))
(defun buffered-reader-fill-buffer (in)
  (symbol-macrolet ((fill (buffered-reader-fill in))
		    (buffer (buffered-reader-buffer in))
		    (stream (buffered-reader-stream in)))
    (setf fill
	  (mod
	   (read-sequence buffer
			  stream
			  :start (mod fill +buffer-size+)
			  :end (+ (mod fill +buffer-size+)
				  (/ +buffer-size+ 2)))
	   +buffer-size+))))

(declaim (inline faster-read-byte))
(defun faster-read-byte (in)
  (declare (type buffered-reader in))
  (symbol-macrolet ((pos  (buffered-reader-pos in))
		    (fill (buffered-reader-fill in))
		    (stream (buffered-reader-stream in))
		    (buffer (buffered-reader-buffer in)))
    (when (= fill pos)
      (buffered-reader-fill-buffer in))
    (when (= fill pos)
      (error 'end-of-file))
    (prog1
	(aref buffer pos)
      (setf pos
	    (mod (1+ pos) +buffer-size+)))))
	   

(declaim (inline hash))
(defun hash (seq)
  (ironclad:digest-sequence +hash+ seq))

(declaim (inline fletcher-16 inverse-fletcher-16))
(defun fletcher-16 (a b byte)
  (declare (type (unsigned-byte 8) byte)
	   (type (mod 255) a b))
  (setf
   a (mod (+ a byte) 255)
   b (mod (+ a b) 255))
  (values a b))
(defun inverse-fletcher-16 (a b window byte)
  (declare (type (unsigned-byte 8) byte)
	   (type (mod 255) a b)
	   (type (mod 17) window)
	   (optimize (safety 0) (speed 3) (debug 0)))
  (setf
   a
   (mod (- a byte -255) 255)
   b (mod (- b (* byte window) +initial-a+ #.(* -255 17)) 255))
  (values a b))

(declaim (inline fast-peek-backwards))
(declaim (type (function (fast-io::output-buffer fixnum) (values octet))
	       fast-peek-backwards))
(defun fast-peek-backwards (buffer amount)
  (declare (type fixnum amount))
  (let ((idx (the fixnum
		  (- (fast-io::output-buffer-fill buffer)
		     amount))))
    (declare (type fixnum idx))
    (if (>= idx 0)
	(aref (fast-io::output-buffer-vector buffer) idx) 
	(or
	 (loop for item of-type octet-vector in (reverse (fast-io::output-buffer-queue buffer))
	    do (incf idx (length item))
	    unless (< idx 0)
	    return (aref item idx))
	 (error "Unable to peek backwards")))))

(setf fast-io:*default-output-buffer-size* +max-length+)

(defun rolling (input)
  (declare (inline fast-read-byte))
  (declare (type buffered-reader input))
  (with-fast-output (buffer)
    (handler-case
	(let ((a +initial-a+)
	      (b +initial-b+)
	      #+sqlitestore::peek2(buf (fast-io:make-octet-vector +min-length+)))
	  (declare (type (unsigned-byte 8) a b))
	  (loop for i from 0 below +min-length+
	     for byte = (faster-read-byte input)
	       do
	       (setf (values a b)
		     (fletcher-16 a b byte))
	       (fast-write-byte byte buffer)
	       #+sqlitestore::peek2(setf (aref buf i) byte))
	  (loop
	     for count from +min-length+
	     for byte = (faster-read-byte input)
	       do
	       (setf (values a b) (inverse-fletcher-16 a b +min-length+
						       #+sqlitestore::peek2(aref buf (mod count +min-length+))
						       #-sqlitestore::peek2(fast-peek-backwards buffer +min-length+))
		     (values a b) (fletcher-16 a b byte))
	       (fast-write-byte byte buffer)
	       #+sqlitestore::peek2(setf (aref buf (mod count +min-length+)) byte)
	     until
	       (or (= a b 0)
		   (>= count (1- +max-length+)))))
      (end-of-file ()))))


;;; database fns TODO move to different file

(defvar *connection*)

(defun next-version (name)
  (let ((version
	 (sqlite:execute-single *connection*
				     "SELECT MAX(version) FROM files WHERE name=?"
				     name)))
    (declare (type (or null (unsigned-byte 64)) version))
    (if version
	(1+ version)
	1)))



(defun add-file (name)
  (let ((v (next-version name)))
    (sqlite:execute-non-query *connection*
			      "INSERT INTO files (name, version) VALUES (?, ?)"
			      name v)
    (sqlite:last-insert-rowid *connection*)))

(defun create-tables ()
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE chunks
                             (id INTEGER PRIMARY KEY, hash BLOB, data BLOB, flags INTEGER)")
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE files
                (id INTEGER PRIMARY KEY, name blob, version INTEGER, size INTEGER)")
  (sqlite:execute-non-query *connection*
			    "CREATE TABLE file_contents
                (id INTEGER, idx INTEGER, contents INTEGER, PRIMARY KEY (id, idx))")
  (sqlite:execute-non-query *connection*
			    "CREATE UNIQUE INDEX idx_chunk_hash ON chunks
                (hash)")
  )

(defun file-id (name version)
  (or
   (sqlite:execute-single *connection*
			  "SELECT id from files WHERE name=? AND version=?"
			  name version)
   (error "Could not locate file ~A version ~a" name version)))


(defconstant +chunk-pipeline-depth+ 256)

(defun prepare-chunk (index data)
  (list index (hash data) (compress data)))

(defun add-chunk (file-id index hash data)
  (sqlite:execute-single *connection*
			 "INSERT INTO file_contents (id, idx, contents) VALUES (?, ?, ?)"
			 file-id index (chunk-id hash data)))

(defun chunk-id (digest data)
  (handler-case
      (progn
	(sqlite:execute-non-query *connection*
				  "INSERT INTO chunks (hash, data) VALUES (?, ?)"
				  digest data)
	(sqlite:last-insert-rowid *connection*))
    (sqlite:sqlite-constraint-error ()
      (sqlite:execute-single
       *connection*
       "SELECT id FROM chunks WHERE hash=?"
       digest))))

(defun add-chunks (file-id input)
  (let ((input (make-buffered-reader :stream input)))
    (sqlite:with-transaction *connection*
      (let ((length 0)
	    (channel (lparallel:make-channel)))
	(declare (type fixnum length))
	(loop
	   with outstanding-tasks fixnum = 0
	   for index fixnum from 0
	   for buf of-type octet-vector = (rolling input)
	   unless (zerop (length buf))
	   do
	     (lparallel:submit-task channel #'prepare-chunk index buf)
	     (incf outstanding-tasks)
	     (incf length (length buf))
	   until (zerop outstanding-tasks)
	   when (or (zerop (length buf))
		    (= outstanding-tasks +chunk-pipeline-depth+))
	   do
	     (apply #'add-chunk file-id (lparallel:receive-result channel))
	     (decf outstanding-tasks))
	(sqlite:execute-non-query *connection*
				  "UPDATE files SET size=? WHERE id=?"
				  length file-id)))))
    
(defun collect-cat-results (queue size output)
  (declare (type (unsigned-byte 64) size))
  (loop
     while (< sofar size)
     for promise = (lparallel.queue:pop-queue queue)
     for chunk of-type (or fast-io:octet-vector null) = (lparallel:force promise)
     unless chunk do (Error "Error fetching chunk:")
     sum (length chunk) into sofar
     do (fast-write-sequence chunk output)))

(defun cat-file (file-id output)
  (let ((length (sqlite:execute-single *connection*
				       "SELECT size FROM files where id=?"
				       file-id))
					(queue (lparallel.queue:make-queue :fixed-capacity +chunk-pipeline-depth+))
	(channel (lparallel:make-channel)))
    ;; propagate errors up
    (lparallel:task-handler-bind ((null #'lparallel:invoke-transfer-error))
      (lparallel:submit-task channel #'collect-cat-results queue length output)
      (loop
	 for index from 0
	 for chunk-id =
	   (sqlite:execute-single *connection*
				  "SELECT contents FROM file_contents WHERE id=? AND idx=?"
				  file-id index)
	 for chunk =
	   (sqlite:execute-single *connection*
				  "SELECT data FROM chunks WHERE id=?"
				  chunk-id)
	 while chunk
	 do (let ((chunk chunk))
	      (lparallel.queue:push-queue (lparallel:future (decompress chunk)) queue)))
      (lparallel.queue:push-queue nil queue)
      (lparallel:receive-result channel))))


(defun stdin-binary ()
  #+sbcl(sb-sys:make-fd-stream 0 :input t
			       :element-type '(unsigned-byte 8))
  #+ccl(ccl::make-fd-stream 0 :direction :input
			   :element-type '(unsigned-byte 8))
  #-(or sbcl ccl)(error "Implement stdin-binary for your implementation"))

(defun stdout-binary ()
  #+sbcl(sb-sys:make-fd-stream 1 :output t
			       :element-type '(unsigned-byte 8))
  #+ccl(ccl::make-fd-stream 1 :direction :output
			    :element-type '(unsigned-byte 8)
			    :sharing :external)
  #-(or ccl sbcl)(error "Implement stdin-binary for your implementation"))
			       
(defun create-command (dbpath)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (sqlite:with-open-database (*connection* dbpath)
      (create-tables))))

(defun add-command (dbpath filename)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (sqlite:with-open-database (*connection* dbpath)
      (add-chunks
       (add-file filename)
       (stdin-binary)))))

(defun restore-command (dbpath filename &optional version)
  (let ((dbpath (uiop:parse-native-namestring dbpath)))
    (declare (optimize (speed 1)))
    (sqlite:with-open-database (*connection* dbpath)
      (let ((version
	     (if version
		 (parse-integer version)
		 (1- (next-version filename))))
	    (stdout-stream (stdout-binary)))
	(fast-io:with-fast-output (stdout stdout-stream)
	  (cat-file (file-id filename version)
		    stdout))
	(finish-output stdout-stream)))))


(defun rolling-bench ()
  (declare (optimize (speed 1)))
  (let ((input (make-buffered-reader :stream (stdin-binary))))
    (loop
       until (zerop (length (rolling input))))))
  
(defun main ()
  ;; TODO be smart about chosing parallelism
  (setf lparallel:*kernel* (lparallel:make-kernel 16))
  (main2 (uiop:command-line-arguments)))

(defun main2 (args)
  (case (make-keyword (string-upcase (car args)))
    #+sbcl(:profile
     (sb-sprof:with-profiling (:report :graph :sample-interval 1/1000 :mode :time)
       (main2 (cdr args)))
     (disassemble #'rolling)
     (disassemble #'inverse-fletcher-16))
    (:add (apply #'add-command (cdr args)))
    (:create (apply #'create-command (cdr args)))
    (:restore (apply #'restore-command (cdr args)))
    (:rolling-bench (apply #'rolling-bench (cdr args)))
    (t (error "Unknown command: ~S" (car args)))))
