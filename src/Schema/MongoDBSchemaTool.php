<?php

namespace Oasis\Mlib\ODM\MongoDB\Schema;

use MongoDB\Model\CollectionInfo;
use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;
use Oasis\Mlib\ODM\MongoDB\Driver\MongoDBManager;

class MongoDBSchemaTool extends AbstractSchemaTool
{
    /**
     * @var array
     */
    protected $dbConfig;

    /** @var MongoDBManager */
    protected $mongoDBManger = null;

    public function createSchema($skipExisting, $dryRun)
    {
        $this->outputWrite("start mongoDB create schema");
        $this->updateTableSchemas($skipExisting, $dryRun);
    }

    public function updateSchema($isDryRun)
    {
        $this->outputWrite("start mongoDB update schema");
    }

    public function dropSchema()
    {
        $mongoTables = $this->getDBManager()->listTables();

        /** @var CollectionInfo $mongoTable */
        foreach ($mongoTables as $mongoTable) {
            $this->outputWrite("start to drop table: {$mongoTable->getName()} ...");
            $this->getDBManager()->dropTable($mongoTable->getName());
        }

        $this->outputWrite("Done.");
    }

    /**
     * @param  array  $dbConfig
     * @return MongoDBSchemaTool
     */
    public function setDbConfig($dbConfig)
    {
        $this->dbConfig = $dbConfig;

        return $this;
    }

    protected function getDBManager()
    {
        if ($this->mongoDBManger !== null) {
            return $this->mongoDBManger;
        }

        $this->mongoDBManger = new MongoDBManager($this->dbConfig);

        return $this->mongoDBManger;
    }

    protected function updateTableSchemas($skipExisting, $dryRun)
    {
        $mongoTables = $this->getDBManager()->listTables();

        /** @var CollectionInfo $mongoTable */
        foreach ($mongoTables as $mongoTable) {
            $this->outputWrite($mongoTable->getName());
        }
    }
}
