<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers\AbstractDbConnection;
use Oasis\Mlib\ODM\Dynamodb\Exceptions\ODMException;
use Oasis\Mlib\ODM\Dynamodb\ItemManager;
use Oasis\Mlib\ODM\MongoDB\Schema\MongoDBSchemaTool;

/**
 * Class MongoDbConnection
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class MongoDbConnection extends AbstractDbConnection
{
    /** @var MongoDBTable */
    private $dbTable = null;

    protected function getDatabaseTable()
    {
        if ($this->dbTable !== null) {
            return $this->dbTable;
        }
        if (empty($this->tableName)) {
            throw new ODMException("Unknown table name to initialize MongoDB client");
        }

        if ($this->itemReflection === null) {
            throw new ODMException("Unknown item reflection to initialize MongoDB client");
        }

        $this->dbTable = new MongoDBTable(
            $this->dbConfig,
            $this->tableName,
            $this->itemReflection
        );

        return $this->dbTable;
    }

    /**
     * @inheritDoc
     */
    public function getSchemaTool(ItemManager $im, $classReflections, callable $outputFunction = null)
    {
        return (new MongoDBSchemaTool($im, $classReflections, $outputFunction))->setDbConfig($this->dbConfig);
    }

    public function batchGet(
        array $keys,
        $isConsistentRead = false,
        $concurrency = 10,
        $projectedFields = [],
        $keyIsTyped = false,
        $retryDelay = 0,
        $maxDelay = 15000
    ) {
        return $this->getDatabaseTable()->batchGet($keys);
    }

    public function batchDelete(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        return $this->getDatabaseTable()->batchDelete($objs);
    }

    public function batchPut(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        return $this->getDatabaseTable()->batchPut($objs);
    }

    public function set(array $obj, $checkValues = [])
    {
        return $this->getDatabaseTable()->set($obj, $checkValues);
    }

    public function get(array $keys, $is_consistent_read = false, $projectedFields = [])
    {
        return $this->getDatabaseTable()->get($keys, $projectedFields);
    }

    public function query(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        &$lastKey = null,
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        return $this->getDatabaseTable()->query(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping,
            $evaluationLimit,
            $projectedFields
        );
    }

    public function queryAndRun(
        callable $callback,
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $stoppedByCallback = false;
        $this->getDatabaseTable()->setQueryLastRawId(null);
        do {
            $resultSet = $this->getDatabaseTable()->query(
                $keyConditions,
                $fieldsMapping,
                $paramsMapping,
                300,
                $projectedFields,
                true
            );
            if (!empty($resultSet)) {
                $stoppedByCallback = false;
                foreach ($resultSet as $item) {
                    if ($stoppedByCallback === true) {
                        return;
                    }
                    $ret = call_user_func($callback, $item);
                    if ($ret === false) {
                        $stoppedByCallback = true;
                    }
                }
            }
        } while (!empty($resultSet) && $stoppedByCallback === false);
        $this->getDatabaseTable()->setQueryLastRawId(null);
    }

    public function queryCount(
        $keyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $isConsistentRead = false,
        $isAscendingOrder = true
    ) {
        return $this->getDatabaseTable()->queryCount(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping
        );
    }

    public function multiQueryAndRun(
        callable $callback,
        $hashKeyName,
        $hashKeyValues,
        $rangeKeyConditions,
        array $fieldsMapping,
        array $paramsMapping,
        $indexName = true,
        $filterExpression = '',
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $concurrency = 10,
        $projectedFields = []
    ) {
        if (!is_array($hashKeyValues)) {
            $hashKeyValues = [$hashKeyValues];
        }

        foreach ($hashKeyValues as $hashKeyValue) {
            $paramsMapping[":{$hashKeyName}"] = $hashKeyValue;
            $fieldsMapping["#{$hashKeyName}"] = $hashKeyName;
            $this->queryAndRun(
                $callback,
                "#{$hashKeyName} = :{$hashKeyName} AND {$rangeKeyConditions}",
                $fieldsMapping,
                $paramsMapping,
                $indexName,
                $filterExpression,
                $isConsistentRead,
                $isAscendingOrder,
                $projectedFields
            );
        }
    }

    public function scan(
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        &$lastKey = null,
        $evaluationLimit = 30,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        return $this->getDatabaseTable()->query(
            $filterExpression,
            $fieldsMapping,
            $paramsMapping,
            $evaluationLimit,
            $lastKey,
            $projectedFields
        );
    }

    public function scanAndRun(
        callable $callback,
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $this->queryAndRun(
            $callback,
            $filterExpression,
            $fieldsMapping,
            $paramsMapping,
            $indexName,
            '',
            $isConsistentRead,
            $isAscendingOrder,
            $projectedFields
        );
    }

    public function parallelScanAndRun(
        $parallel,
        callable $callback,
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $isAscendingOrder = true,
        $projectedFields = []
    ) {
        $this->queryAndRun(
            $callback,
            $filterExpression,
            $fieldsMapping,
            $paramsMapping,
            $indexName,
            '',
            $isConsistentRead,
            $isAscendingOrder,
            $projectedFields
        );
    }

    public function scanCount(
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $parallel = 10
    ) {
        return $this->getDatabaseTable()->queryCount(
            $filterExpression,
            $fieldsMapping,
            $paramsMapping
        );
    }
}
