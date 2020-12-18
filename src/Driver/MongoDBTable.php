<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use MongoDB\Model\BSONDocument;
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


    public function __construct(array $dbConfig, $tableName, ItemReflection $itemReflection)
    {
        $this->dbCollection = (new Client($dbConfig['endpoint']))
            ->selectDatabase($dbConfig['database'])
            ->selectCollection($tableName);

        $this->tableName      = $tableName;
        $this->itemReflection = $itemReflection;
        $this->attributeTypes = $itemReflection->getAttributeTypes();
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

        $ret = $this->getArrayElements($doc, $lastId);

        if (empty($ret)) {
            return $ret;
        }
        else {
            return $ret[0];
        }
    }

    protected function getArrayElements(Cursor $cursor, &$lastId)
    {
        if ($cursor === null) {
            return [];
        }

        $arr = $cursor->toArray();

        if (empty($arr)) {
            return [];
        }

        $retList = [];
        foreach ($arr as $item) {
            /** @var BSONDocument $bsonDoc */
            $bsonDoc = $item;
            $ret     = $bsonDoc->exchangeArray([]);
            $lastId  = $ret['_id'];
            unset($ret['_id']);
            $retList[] = $ret;
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
        $this->dbCollection->findOneAndUpdate(
            $this->itemReflection->getPrimaryKeys($obj),
            [
                '$set' => $obj,
            ],
            [
                'upsert' => true,
                'todo'   => $checkValues  // remove later
            ]
        );

        // todo: implement check and set
        return true;
    }

    public function batchGet(array $keys)
    {
        $doc = $this->dbCollection->find(
            [
                '$or' => $keys,
            ]
        );

        return $this->getArrayElements($doc, $lastId);
    }

    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $evaluationLimit,
        &$lastId = 0,
        $projectedFields = []
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

        if (!empty($lastId)) {
            $filter['_id'] = ['$gt' => $lastId];
        }

        if (!empty($projectedFields)) {
            $options['projection'] = $this->getProjectionOption($projectedFields);
        }

        $doc = $this->dbCollection->find(
            $filter,
            $options
        );

        return $this->getArrayElements($doc, $lastId);
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

}
