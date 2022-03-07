(ns task02.network
  (:use [task02 helpers query])
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net Socket ServerSocket InetAddress InetSocketAddress SocketTimeoutException]))

;; Объявить переменную для синхронизации между потоками. Воспользуйтесь promise
(def ^:private should-be-finished (promise))

;; Hint: *in*, *out*, io/writer, io/reader, socket.getOutputStream(), socket.getInputStream(), socket.close(), binding
;;       deliver, prn
(defn handle-request [^Socket sock]
  ;; переопределить *in* & *out* чтобы они указывали на входной и выходной потоки сокета
  (binding [*in* (io/reader (.getInputStream sock))
        *out*  (.getOutputStream sock)]
   (try
     (let [s (read-line)] ;; считать данные из переопределенного *in*
       (if (= (str/lower-case s) "quit")
         (deliver should-be-finished true)
         (io/writer *out* (perform-query *in*))
         ;;; 2) выполнить запрос при помощи perform-query и записать
               ;;; результат в переопределенный *out*
         ))
     (catch Throwable ex
       (println "Exception: " ex))
     (finally
       (.close sock)))))


;; Hint: future, deliver
(defn- run-loop [server-sock]
  (try
    (let [^Socket sock (.accept server-sock)]
      (future (handle-request sock)))
    (catch SocketTimeoutException ex)
    (catch Throwable ex
      (println "Got exception" ex)
      (deliver should-be-finished true))))

(defn run [port]
  (let [sock-addr (InetSocketAddress. "localhost" port)
        server-socket (doto (ServerSocket.)
                        (.setReuseAddress true)
                        (.setSoTimeout 3000)
                        (.bind sock-addr))]
    (loop [_ (run-loop server-socket)]
      (when-not (realized? should-be-finished) ;; следующий запрос если работа не завершается...
        (recur (run-loop server-socket))))
    (.close server-socket)))
