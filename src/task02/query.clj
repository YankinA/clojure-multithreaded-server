(ns task02.query
  (:use [task02 helpers db]))
(require '[clojure.string :as str])

;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil

(defn make-where-function [& args] 
  (fn [field] (let [[key-str sing-str value-str] args
                    field-value ((keyword key-str) field)
                    sing (load-string sing-str)
                    value (read-string value-str)
                    bool-res (sing field-value value)]
                bool-res)))

(defn parse-query [query index sel]
  (case query
    "where" [:where (make-where-function
              (get sel (+ index 1))
              (get sel (+ index 2))
              (get sel (+ index 3)))]
    "order" [:order-by (keyword (get sel (+ index 2)))]
    "limit" [:limit (read-string (get sel (+ index 1)))]
    "join" [:joins [[(keyword (get sel (+ index 3)))
                     (get sel (+ index 1))
                     (keyword (get sel (+ index 5)))]]]
    nil))


(defn parse-select [^String sel-string]
  (if (clojure.string/includes? sel-string "select")
    (let [sel (clojure.string/split sel-string #" ")
          table-name (get sel 1)
          sel-indexed (map-indexed #(list % %2) sel)]
      (reduce (fn [acc [index query]]
                (let [parsend-query  (parse-query query index sel)]
                  (if parsend-query (apply conj acc parsend-query) acc)))
              [table-name] sel-indexed))
    nil))

 
(parse-select 
 "select student where id = 10 order by year limit 5 join subject on id = sid"
 )

 


;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...


(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
