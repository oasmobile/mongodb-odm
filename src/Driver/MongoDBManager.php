<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Database;
use MongoDB\Model\CollectionInfo;
use Oasis\Mlib\ODM\MongoDB\Schema\Structures\Index;
use Oasis\Mlib\ODM\MongoDB\Schema\Structures\Table;

/**
 * Class MongoDBManager
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class MongoDBManager
{
    /** @var Database */
    protected $database;

    /** @var array */
    protected $dbConfig;

    public function __construct(array $dbConfig)
    {
        $this->dbConfig = $dbConfig;
        $this->database = (new Client($dbConfig['endpoint']))
            ->selectDatabase($dbConfig['database']);
    }

    /**
     * @return Database
     */
    public function getDatabase(): Database
    {
        return $this->database;
    }

    /**
     * @return bool
     */
    public function isDebug(): bool
    {
        if (strpos($this->dbConfig['database'], 'odm-ut') !== false) {
            return true;
        }
        else {
            return false;
        }
    }

    public function listTables()
    {
        $collections = $this->database->listCollections();
        $tableList   = [];

        /** @var CollectionInfo $item */
        foreach ($collections as $item) {
            $tableList[$item->getName()] = $this->createTableWithCollection($item);
        }

        return $tableList;
    }

    protected function createTableWithCollection(CollectionInfo $item)
    {
        $collection = $this->database->selectCollection($item->getName());
        $indexList  = $collection->listIndexes();

        $table = new Table();
        $table->setName($item->getName());

        foreach ($indexList as $indexInfo) {
            if ($indexInfo->getName() == '_id_') {
                continue;
            }

            $table->appendIndex(
                (new Index())
                    ->setName($indexInfo->getName())
                    ->setColumns(array_keys($indexInfo->getKey()))
            );
        }

        return $table;
    }

    public function dropTable($tableName)
    {
        $this->database->dropCollection($tableName);
    }

}
