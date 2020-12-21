<?php

namespace Oasis\Mlib\ODM\MongoDB\Schema;

use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;

class MongoDBSchemaTool extends AbstractSchemaTool
{
    /**
     * @var array
     */
    protected $dbConfig;

    public function createSchema($skipExisting, $dryRun)
    {
        $this->outputWrite("start mongoDB create schema");
    }

    public function updateSchema($isDryRun)
    {
        $this->outputWrite("start mongoDB update schema");
    }

    public function dropSchema()
    {
        $this->outputWrite("start mongoDB drop schema");
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
}
