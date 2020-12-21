<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use MongoDB\Client;
use MongoDB\Database;

/**
 * Class MongoDBManager
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class MongoDBManager
{
    /** @var Database */
    protected $database;

    public function __construct(array $dbConfig)
    {
        $this->database = (new Client($dbConfig['endpoint']))
            ->selectDatabase($dbConfig['database']);
    }


    public function listTables()
    {
        return $this->database->listCollections();
    }

    public function dropTable($tableName)
    {
        $this->database->dropCollection($tableName);
    }

}
