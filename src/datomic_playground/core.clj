(ns datomic-playground.core
  (:require [datomic.client.api :as d]))

;;;; Setup

(def db-name "composite-test")

(def client
  "This function will return a local implementation of the client
   interface when run on a Datomic compute node. If you want to call
   locally, fill in the correct values in the map."
  #(d/client {:server-type :ion
              :region "us-west-2"
              :system "your-system"
              :query-group "your-system"
              :endpoint "http://entry.your-system.us-west-2.datomic.net:8182/"
              :proxy-port 8182}))

(defn conn [] (d/connect (client) {:db-name db-name}))

;;;; Base attributes

(def base-attrs
  [{:db/ident :country/code
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Country code."}
   {:db/ident :document/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Document ID."}
   {:db/ident :document/country
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "Country where the document was created."}
   {:db/ident :document/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Document type: :invoice, :credit-note, etc."}
   {:db/ident :document/vendor-tax-id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Document vendor tax ID."}
   {:db/ident :document/fiscal-year
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Document fiscal year."}
   {:db/ident :document/number
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Document unique number."}
   {:db/ident :document/environment
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Document environment: :live or :test."}])

(def test-country [{:country/code "PE"}])

(def composite-attr
  [{:db/ident :document/country+type+vendor-tax-id+fiscal-year+number
    :db/valueType :db.type/tuple
    :db/tupleAttrs [:document/country
                    :document/type
                    :document/vendor-tax-id
                    :document/fiscal-year
                    :document/number]
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}])

;;;; New composite attribute

(def new-composite-attr
  [{:db/ident :document/country+type+vendor-tax-id+fiscal-year+number+environment
    :db/valueType :db.type/tuple
    :db/tupleAttrs [:document/country
                    :document/type
                    :document/vendor-tax-id
                    :document/fiscal-year
                    :document/number
                    :document/environment]
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}])

(def test-entity
  [{:document/id "1"
    :document/country [:country/code "PE"]
    :document/type :invoice
    :document/vendor-tax-id "123"
    :document/fiscal-year "2019"
    :document/number "F001-1"
    :document/environment :test}])

(def test-entity-new-environment
  [{:document/id "2"
    :document/country [:country/code "PE"]
    :document/type :invoice
    :document/vendor-tax-id "123"
    :document/fiscal-year "2019"
    :document/number "F001-1"
    :document/environment :live}])

(comment
  ;;; Setup db
  (d/create-database (get-client) {:db-name db-name})

  ;;; Install basic attributes
  (d/transact (conn) {:tx-data  base-attrs})
  
  ;;; Create country
  (d/transact (conn) {:tx-data  test-country})

  ;;; Install the first composite attribute
  (d/transact (conn) {:tx-data  composite-attr})

  ;;; Transact test data
  (d/transact (conn) {:tx-data test-entity})

  ;;; Install the new composite attribute
  (d/transact (conn) {:tx-data new-composite-attr})

  ;;; Try to transact another entity, with a differente environment
  ;;; This throws a Unique conflict: :document/country+type+vendor-tax-id+fiscal-year+number, value: [24378371811049553 :invoice "123" "2019" "F001-1"] already held by: ...
  ;;; Datomic is giving precedence to the composite attribute that was first transacted
  (d/transact (conn) {:tx-data test-entity-new-environment})

  ;;; Query entities that use the first composite attribute :document/country+type+vendor-tax-id+fiscal-year+number. This returns the entity we transacted before.

  (d/q '[:find (pull ?e pull-pattern?)
         :in $ pull-pattern?
         :where [?e :document/country+type+vendor-tax-id+fiscal-year+number]]
       (d/db (conn))
       [:document/id
        {:document/country [:country/id]}
        :document/type
        :document/vendor-tax-id
        :document/fiscal-year
        :document/environment])

    ;;; Query entities that use the first composite attribute :document/country+type+vendor-tax-id+fiscal-year+number+environment. This returns an empty seq.

  (def documents
    (d/q '[:find (pull ?e pull-pattern?)
           :in $ pull-pattern?
           :where [?e :document/country+type+vendor-tax-id+fiscal-year+number]]
         (d/db (conn))
         [:document/id
          {:document/country [:country/id]}
          :document/type
          :document/vendor-tax-id
          :document/fiscal-year
          :document/environment]))

  ;;; Let's retract and re-assert all involved attributes, including environment
  ;;; so I can force the new composite attribute to be built.

  (d/transact (conn) {:tx-data [[:db/retract
                               [:document/id "1"]
                               :document/country [:country/code "PE"]]
                              [:db/retract
                               [:document/id "1"]
                               :document/type :invoice]
                              [:db/retract
                               [:document/id "1"]
                               :document/vendor-tax-id "123"]
                              [:db/retract
                               [:document/id "1"]
                               :document/fiscal-year "2019"]
                              [:db/retract
                               [:document/id "1"]
                               :document/environment :test]]})

  (d/transact (conn) {:tx-data test-entity})

  ;;; Then try to transact a new entity with the same attributes but different environment.
  ;;; This still returns "Unique conflict: :document/country+type+vendor-tax-id+fiscal-year+number, value: [24378371811049553 :invoice \"123\" \"2019\" \"F001-1\"]
  ;;; Datomic is giving precedence to the less specific composite attribute

  (d/transact (conn) {:tx-data test-entity-new-environment})
  )
