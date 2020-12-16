<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Collection;
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
    }

}
