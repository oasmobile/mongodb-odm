<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use Oasis\Mlib\ODM\Dynamodb\DBAL\Drivers\AbstractDbConnection;
use Oasis\Mlib\ODM\Dynamodb\ItemManager;

/**
 * Class MongoDbConnection
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class MongoDbConnection extends AbstractDbConnection
{

    /**
     * @inheritDoc
     */
    public function getSchemaTool(ItemManager $im, $classReflections, callable $outputFunction = null)
    {
        // TODO: Implement getSchemaTool() method.
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
        // TODO: Implement batchGet() method.
    }

    public function batchDelete(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        // TODO: Implement batchDelete() method.
    }

    public function batchPut(array $objs, $concurrency = 10, $maxDelay = 15000)
    {
        // TODO: Implement batchPut() method.
    }

    public function set(array $obj, $checkValues = [])
    {
        // TODO: Implement set() method.
    }

    public function get(array $keys, $is_consistent_read = false, $projectedFields = [])
    {
        // TODO: Implement get() method.
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
        // TODO: Implement query() method.
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
        // TODO: Implement queryAndRun() method.
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
        // TODO: Implement queryCount() method.
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
        // TODO: Implement multiQueryAndRun() method.
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
        // TODO: Implement scan() method.
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
        // TODO: Implement scanAndRun() method.
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
        // TODO: Implement parallelScanAndRun() method.
    }

    public function scanCount(
        $filterExpression = '',
        array $fieldsMapping = [],
        array $paramsMapping = [],
        $indexName = true,
        $isConsistentRead = false,
        $parallel = 10
    ) {
        // TODO: Implement scanCount() method.
    }
}
