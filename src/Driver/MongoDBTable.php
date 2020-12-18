<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use MongoDB\Model\BSONDocument;
use Oasis\Mlib\ODM\Dynamodb\ItemReflection;

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

    public function get(array $keys)
    {
        $doc = $this->dbCollection->find(
            $keys,
            [
                'limit' => 1,
            ]
        );

        $ret = $this->getArrayElements($doc);

        if (empty($ret)) {
            return $ret;
        }
        else {
            return $ret[0];
        }
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

        return $this->getArrayElements($doc);
    }

    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $evaluationLimit = 30
    ) {
        $doc = $this->dbCollection->find(
            (new QueryConditionWrapper($keyConditions, $fieldsMapping, $paramsMapping))->getFilter(),
            [
                'limit' => $evaluationLimit,
            ]
        );

        return $this->getArrayElements($doc);
    }

    protected function getArrayElements(Cursor $cursor)
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
            unset($ret['_id']);
            $retList[] = $ret;
        }

        return $retList;
    }

}
