package document_test

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	. "github.com/kphelps/fdb-document-layer/document"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestNestedModel struct {
	X string
}

type TestModel struct {
	ID                string
	Name              string
	Count             int
	Bool              bool
	Numbers           []int64
	Strings           []string
	KeyValues         map[string]string
	Nested            TestNestedModel
	MapStructs        map[string]TestNestedModel
	MapPointerStructs map[string]*TestNestedModel
}

var _ = Describe("DocumentStorage", func() {

	var db fdb.Database
	var dir directory.DirectorySubspace
	var storage *DocumentStorage

	BeforeEach(func() {
		fdb.MustAPIVersion(200)
		newDB, err := fdb.OpenDefault()
		Expect(err).NotTo(HaveOccurred())
		db = newDB
		dir, err = directory.CreateOrOpen(db, []string{"test"}, nil)
		Expect(err).NotTo(HaveOccurred())
		storage = NewDocumentStorage(dir.Sub())
	})

	AfterEach(func() {
		db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			tx.Clear(dir)
			return nil, nil
		})
	})

	Describe("Save", func() {

		var model TestModel

		BeforeEach(func() {
			model = TestModel{
				ID:        "abc",
				Name:      "Name",
				Count:     10,
				Bool:      true,
				Numbers:   []int64{1, 2, 3},
				Strings:   []string{"hello", "world"},
				KeyValues: map[string]string{"hello": "world", "x": "y"},
				Nested:    TestNestedModel{"Hello"},
				MapStructs: map[string]TestNestedModel{
					"hello": TestNestedModel{"X"},
				},
				MapPointerStructs: map[string]*TestNestedModel{
					"hello": &TestNestedModel{"Z"},
				},
			}
		})

		It("should find what was saved", func() {
			_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, storage.Save(tx, model)
			})
			Expect(err).NotTo(HaveOccurred())
			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				newModel := TestModel{ID: model.ID}
				return &newModel, storage.Find(tx, &newModel)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(*out.(*TestModel)).To(Equal(model))
		})

		It("Should update an existing document", func() {
			_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, storage.Save(tx, model)
			})
			Expect(err).NotTo(HaveOccurred())
			model.Name = "Hello"
			model.Count = 100
			model.Bool = false
			model.Numbers = []int64{}
			model.Strings = []string{"hello", "world", "more"}
			model.KeyValues = map[string]string{"hello": "world2", "z": "a", "b": "x"}
			model.Nested = TestNestedModel{"World"}
			model.MapStructs = map[string]TestNestedModel{
				"Blah":  TestNestedModel{"X"},
				"hello": TestNestedModel{"Z"},
			}
			model.MapPointerStructs = map[string]*TestNestedModel{}
			_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, storage.Save(tx, model)
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				newModel := TestModel{ID: model.ID}
				return &newModel, storage.Find(tx, &newModel)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(*out.(*TestModel)).To(Equal(model))
		})
	})
})
