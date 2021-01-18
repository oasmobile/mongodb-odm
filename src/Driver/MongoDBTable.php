<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use MongoDB\Model\BSONDocument;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\DataConsistencyException;
use Oasis\Mlib\ODM\Dynamodb\ItemReflection;
use Oasis\Mlib\Utils\Exceptions\DataValidationException;

/**
 * Class MongoDBTable
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class MongoDBTable
{
    /** @var string */
    private $tableName;

    /** @var array */
    private $attributeTypes;

    /** @var ItemReflection */
    private $itemReflection;

    /** @var Collection */
    private $dbCollection;

    private $queryLastRawId = null;

    public function __construct(array $dbConfig, $tableName, ItemReflection $itemReflection)
    {
        $this->dbCollection = (new Client($dbConfig['endpoint']))
            ->selectDatabase($dbConfig['database'])
            ->selectCollection($tableName);

        $this->tableName      = $tableName;
        $this->itemReflection = $itemReflection;
        $this->attributeTypes = $itemReflection->getAttributeTypes();
    }

    /**
     */
    public function getQueryLastRawId()
    {
        return $this->queryLastRawId;
    }


    public function setQueryLastRawId($queryLastRawId)
    {
        $this->queryLastRawId = $queryLastRawId;
    }

    public function get(array $keys, $projectedFields = [])
    {
        $options = [
            'limit' => 1,
        ];

        if (!empty($projectedFields)) {
            $options['projection'] = $this->getProjectionOption($projectedFields);
        }

        $doc = $this->dbCollection->find(
            $keys,
            $options
        );

        $ret = $this->getArrayElements($doc);

        if (empty($ret)) {
            return null;
        }
        else {
            return $ret[0];
        }
    }

    protected function getProjectionOption(array $projectedFields)
    {
        if (empty($projectedFields)) {
            throw new DataValidationException("projected fields is empty");
        }

        $fields = array_values($projectedFields);
        $ret    = [];

        foreach ($fields as $field) {
            $ret[$field] = true;
        }

        return $ret;
    }

    protected function getArrayElements(Cursor $cursor, $saveLastRowId = false)
    {
        if ($cursor === null) {
            return [];
        }

        $arr     = $cursor->toArray();
        $retList = [];

        if (empty($arr)) {
            return $retList;
        }

        /** @var BSONDocument $bsonDoc */
        foreach ($arr as $bsonDoc) {
            $ret = $bsonDoc->exchangeArray([]);
            if ($saveLastRowId) {
                $this->queryLastRawId = $ret['_id'];
            }
            unset($ret['_id']);
            $retList[] = array_map([$this, 'cbNormalizeBsonDc'], $ret);
        }

        return $retList;
    }

    public function batchDelete(array $objs)
    {
        foreach ($objs as $obj) {
            $this->dbCollection->deleteOne(
                $this->itemReflection->getPrimaryKeys($obj)
            );
        }

        return true;
    }

    public function batchPut(array $objs)
    {
        foreach ($objs as $obj) {
            $this->set($obj);
        }

        return true;
    }

    public function set(array $obj, $checkValues = [])
    {
        $filter = $this->itemReflection->getPrimaryKeys($obj);
        $upsert = true;
        $cv     = $this->getCheckValues($checkValues);
        if (!empty($cv)) {
            $filter = array_merge($filter, $cv);
            $upsert = false;
        }

        $ret = $this->dbCollection->findOneAndUpdate(
            $filter,
            [
                '$set' => $obj,
            ],
            [
                'upsert' => $upsert,
            ]
        );

        if (!empty($cv) && $ret == null) {
            throw new DataConsistencyException();
        }

        return $ret;
    }

    protected function getCheckValues($checkValues)
    {
        if (empty($checkValues)) {
            return [];
        }

        foreach ($checkValues as $key => $value) {
            if ($value === null) {
                return [];
            }
        }

        return $checkValues;
    }

    public function batchGet(array $keys)
    {
        $doc = $this->dbCollection->find(
            [
                '$or' => $keys,
            ]
        );

        return $this->getArrayElements($doc);
    }

    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $evaluationLimit,
        $projectedFields = [],
        $saveLastRowId = false
    ) {
        $filter = (new QueryConditionWrapper(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping,
            $this->itemReflection->getAttributeTypes()
        ))->getFilter();

        $options = [
            'limit' => $evaluationLimit,
        ];

        if (!empty($this->queryLastRawId)) {
            $filter['_id'] = ['$gt' => $this->queryLastRawId];
        }

        if (!empty($projectedFields)) {
            $options['projection'] = $this->getProjectionOption($projectedFields);
        }

        $doc = $this->dbCollection->find(
            $filter,
            $options
        );

        return $this->getArrayElements($doc, $saveLastRowId);
    }

    public function queryCount(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping
    ) {
        return $this->dbCollection->countDocuments(
            (new QueryConditionWrapper(
                $keyConditions,
                $fieldsMapping,
                $paramsMapping,
                $this->itemReflection->getAttributeTypes()
            ))->getFilter()
        );
    }

    protected function cbNormalizeBsonDc($valItem)
    {
        if ($valItem instanceof \ArrayObject) {
            $valItem = $valItem->exchangeArray([]);
        }

        if (is_array($valItem)) {
            $valItem = array_map([$this, 'cbNormalizeBsonDc'], $valItem);
        }

        return $valItem;
    }

}
