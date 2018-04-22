package document

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type DocumentStorage struct {
	root        subspace.Subspace
	schemaCache map[reflect.Type]*modelCache
}

func NewDocumentStorage(space subspace.Subspace) *DocumentStorage {
	return &DocumentStorage{
		root:        space,
		schemaCache: make(map[reflect.Type]*modelCache),
	}
}

type ExampleDocument struct {
	ID string
}

func (storage *DocumentStorage) Save(
	tx fdb.Transaction,
	value interface{},
) error {
	valueType := reflect.TypeOf(value)
	if valueType.Kind() != reflect.Struct {
		return errors.New("Bad save")
	}
	collection := valueType.Name()
	id := storage.getStructID(value)
	return storage.SaveRaw(tx, collection, id, value)
}

func (storage *DocumentStorage) Find(
	tx fdb.Transaction,
	value interface{},
) error {
	prefix := storage.modelIDPrefix(value)
	return storage.loadRec(tx, prefix, value)
}

func (storage *DocumentStorage) loadRec(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	switch reflect.TypeOf(value).Elem().Kind() {
	case reflect.Ptr:
		v := reflect.ValueOf(value).Elem()
		x := reflect.New(v.Type().Elem())
		reflect.ValueOf(value).Elem().Set(x)
		return storage.loadRec(tx, prefix, v.Interface())
	case reflect.Struct:
		return storage.loadStruct(tx, prefix, value)
	case reflect.Slice:
		return storage.loadSlice(tx, prefix, value)
	case reflect.Array:
		return storage.loadArray(tx, prefix, value)
	case reflect.Map:
		return storage.loadMap(tx, prefix, value)
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:

		return storage.loadKeyValue(tx, prefix, value)
	}
	return errors.New(fmt.Sprintf("Unsuppoorted type: %s", reflect.TypeOf(value)))
}

func (storage *DocumentStorage) loadKeyValue(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	key := storage.root.Pack(prefix)
	bytes, err := tx.Get(key).Get()
	if err != nil {
		return err
	}
	return storage.unpackValue(bytes, value)
}

func (storage *DocumentStorage) unpackValue(
	bytes []byte,
	out interface{},
) error {
	tupleValues, err := tuple.Unpack(bytes)
	if err != nil {
		return err
	}

	outType := reflect.TypeOf(out)
	if len(tupleValues) > 1 {
		return errors.New("Invalid value found")
	} else if len(tupleValues) == 1 {
		tupleValue := reflect.ValueOf(tupleValues[0])
		convertedValue := tupleValue.Convert(outType.Elem())
		reflect.ValueOf(out).Elem().Set(convertedValue)
	} else {
		reflect.ValueOf(out).Elem().Set(reflect.New(outType.Elem()))
	}
	return nil

}

func (storage *DocumentStorage) loadStruct(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	schema := storage.modelSchema(value)
	for _, field := range schema.Fields() {
		fullKey := append(prefix, field.Name)
		if err := storage.loadRec(tx, fullKey, field.GetValuePointer(value)); err != nil {
			return err
		}
	}
	return nil
}

func (storage *DocumentStorage) loadSlice(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	arraySpace := storage.root.Sub(prefix...)
	arrayIterator := tx.GetRange(arraySpace, fdb.RangeOptions{}).Iterator()
	outValue := reflect.ValueOf(value)
	sliceValue := outValue.Elem()
	elemType := sliceValue.Type().Elem()

	for arrayIterator.Advance() {
		kv := arrayIterator.MustGet()
		value := reflect.New(elemType)
		if err := storage.unpackValue(kv.Value, value.Interface()); err != nil {
			return err
		}
		sliceValue = reflect.Append(sliceValue, value.Elem())
	}
	if sliceValue.IsNil() {
		sliceValue = reflect.MakeSlice(sliceValue.Type(), 0, 0)
	}
	outValue.Elem().Set(sliceValue)
	return nil
}

func (storage *DocumentStorage) loadArray(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	// this probably doesn't work but who cares, arrays are dumb
	return storage.loadSlice(tx, prefix, value)
}

func (storage *DocumentStorage) loadMap(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	mapSpace := storage.root.Sub(prefix...)
	mapIterator := tx.GetRange(mapSpace, fdb.RangeOptions{}).Iterator()
	outValue := reflect.ValueOf(value)
	mapValue := outValue.Elem()
	mapValue.Set(reflect.MakeMap(mapValue.Type()))
	keyType := mapValue.Type().Key()
	valueType := mapValue.Type().Elem()

	for mapIterator.Advance() {
		kv := mapIterator.MustGet()
		key := reflect.New(keyType)
		value := reflect.New(valueType)
		keyTuple, err := storage.root.Unpack(kv.Key)
		if err != nil {
			return err
		}
		mapKeyIndex := len(prefix)
		convertedKey := reflect.ValueOf(keyTuple[mapKeyIndex]).Convert(keyType)
		key.Elem().Set(convertedKey)

		valueKeyTuple := keyTuple[0 : mapKeyIndex+1]
		if err := storage.loadRec(tx, valueKeyTuple, value.Interface()); err != nil {
			return err
		}

		mapValue.SetMapIndex(key.Elem(), value.Elem())
	}
	return nil
}

func (storage *DocumentStorage) modelSchema(value interface{}) *modelCache {
	tipe := reflect.TypeOf(value)
	if tipe.Kind() == reflect.Ptr {
		tipe = tipe.Elem()
	}
	cache, found := storage.schemaCache[tipe]
	if !found {
		cache = newModelCache(tipe)
		storage.schemaCache[tipe] = cache
	}
	return cache
}

func (storage *DocumentStorage) getStructID(value interface{}) interface{} {
	return storage.modelSchema(value).GetID(value)
}

func (storage *DocumentStorage) modelIDPrefix(value interface{}) tuple.Tuple {
	return tuple.Tuple{
		storage.modelSchema(value).Collection(),
		storage.getStructID(value),
	}
}

func (storage *DocumentStorage) SaveRaw(
	tx fdb.Transaction,
	collection string,
	id interface{},
	value interface{},
) error {
	prefix := tuple.Tuple{collection, id}
	return storage.saveRec(tx, prefix, value)
}

func (storage *DocumentStorage) saveRec(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Ptr:
		return storage.saveRec(tx, prefix, reflect.ValueOf(value).Elem().Interface())
	case reflect.Struct:
		return storage.saveStruct(tx, prefix, value)
	case reflect.Slice:
		return storage.saveSlice(tx, prefix, value)
	case reflect.Array:
		return storage.saveArray(tx, prefix, value)
	case reflect.Map:
		return storage.saveMap(tx, prefix, value)
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:

		return storage.setKeyValue(tx, prefix, value)
	}
	return errors.New(fmt.Sprintf("Unsuppoorted type: %s", reflect.TypeOf(value)))
}

func (storage *DocumentStorage) setKeyValue(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	key := storage.root.Pack(prefix)
	valueTuple := tuple.Tuple{value}
	tx.Set(key, valueTuple.Pack())
	return nil
}

func (storage *DocumentStorage) saveStruct(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	schema := storage.modelSchema(value)
	for _, field := range schema.Fields() {
		fullKey := append(prefix, field.Name)
		if err := storage.saveRec(tx, fullKey, field.GetValue(value)); err != nil {
			return err
		}
	}
	return nil
}

func (storage *DocumentStorage) saveSlice(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	v := reflect.ValueOf(value)
	tx.ClearRange(storage.root.Sub(prefix...))
	for i := 0; i < v.Len(); i++ {
		key := append(prefix, int64(i))
		element := v.Index(i).Interface()
		if err := storage.saveRec(tx, key, element); err != nil {
			return err
		}
	}
	return nil
}

func (storage *DocumentStorage) saveArray(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	return storage.saveSlice(tx, prefix, value)
}

func (storage *DocumentStorage) saveMap(
	tx fdb.Transaction,
	prefix tuple.Tuple,
	value interface{},
) error {
	v := reflect.ValueOf(value)
	tx.ClearRange(storage.root.Sub(prefix...))
	for _, mapKey := range v.MapKeys() {
		key := append(prefix, mapKey.Interface())
		element := v.MapIndex(mapKey).Interface()
		if err := storage.saveRec(tx, key, element); err != nil {
			return err
		}
	}
	return nil
}

type modelCache struct {
	collection string
	idIndex    []int
	fields     []*fieldCache
}

func newModelCache(modelType reflect.Type) *modelCache {
	return &modelCache{
		collection: modelType.Name(),
		idIndex:    getIDField(modelType),
		fields:     cacheFields(modelType),
	}
}

func getIDField(modelType reflect.Type) []int {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tags := strings.Split(field.Tag.Get("doc"), ";")
		for _, tag := range tags {
			if tag == "id" {
				return field.Index
			}
		}
	}
	field, ok := modelType.FieldByName("ID")
	if !ok {
		return nil
	}
	return field.Index
}

func cacheFields(modelType reflect.Type) []*fieldCache {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	out := make([]*fieldCache, 0)
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		if cachedField := cacheField(field); cachedField != nil {
			out = append(out, cachedField)
		}
	}
	return out
}

func cacheField(fieldType reflect.StructField) *fieldCache {
	return &fieldCache{
		FieldIndex: fieldType.Index,
		Name:       fieldType.Name,
	}
}

func (cache *modelCache) Collection() string {
	return cache.collection
}

func (cache *modelCache) GetID(value interface{}) interface{} {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.FieldByIndex(cache.idIndex).Interface()
}

func (cache *modelCache) Fields() []*fieldCache {
	return cache.fields
}

type fieldCache struct {
	FieldIndex []int
	Name       string
}

func (cache *fieldCache) GetValuePointer(value interface{}) interface{} {
	v := reflect.ValueOf(value)
	return v.Elem().FieldByIndex(cache.FieldIndex).Addr().Interface()
}

func (cache *fieldCache) GetValue(value interface{}) interface{} {
	return reflect.ValueOf(value).FieldByIndex(cache.FieldIndex).Interface()
}
