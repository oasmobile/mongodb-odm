<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Collection;
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

    public function get(array $keys)
    {
        $doc = $this->dbCollection->find(
            $keys,
            [
                'limit' => 1,
            ]
        );

        if ($doc === null) {
            return null;
        }

        $arr = $doc->toArray();

        if (empty($arr) || empty($arr[0])) {
            return null;
        }

        /** @var BSONDocument $bsonDoc */
        $bsonDoc = $arr[0];
        $ret     = $bsonDoc->exchangeArray([]);

        unset($ret['_id']);

        return $ret;
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

    public function batchGet(array $keys)
    {
        $doc = $this->dbCollection->find(
            [
                '$or' => $keys,
            ]
        );

        if ($doc === null) {
            return [];
        }

        $arr = $doc->toArray();

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
