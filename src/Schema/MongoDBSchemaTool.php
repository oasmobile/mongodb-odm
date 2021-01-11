<?php

namespace Oasis\Mlib\ODM\MongoDB\Schema;

use MongoDB\Model\CollectionInfo;
use Oasis\Mlib\ODM\Dynamodb\DBAL\Schema\AbstractSchemaTool;
use Oasis\Mlib\ODM\Dynamodb\ItemReflection;
use Oasis\Mlib\ODM\MongoDB\Driver\MongoDBManager;
use Oasis\Mlib\ODM\MongoDB\Schema\Structures\Index;
use Oasis\Mlib\ODM\MongoDB\Schema\Structures\Table;
use Oasis\Mlib\ODM\MongoDB\Schema\Structures\ComparableItem;

/**
 * Class MongoDBSchemaTool
 * @package Oasis\Mlib\ODM\MongoDB\Schema
 */
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
        $this->updateTableSchemas($skipExisting, $dryRun);
    }

    public function updateSchema($isDryRun)
    {
        $this->updateTableSchemas(false, $isDryRun);
    }

    /**
     * !! Attention
     * Drop table command is so danger we only provided in develop environment
     */
    public function dropSchema()
    {
        if ($this->getDBManager()->isDebug() !== true) {
            $this->outputWrite("Table drop command only available in develop environment.");

            return;
        }

        $mongoTables = $this->getDBManager()->listTables();

        /** @var CollectionInfo $mongoTable */
        foreach ($mongoTables as $mongoTable) {
            $this->outputWrite("Will drop table: {$mongoTable->getName()}");
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

    /** @noinspection PhpUnusedParameterInspection */
    protected function updateTableSchemas($skipExisting, $dryRun)
    {
        $mongoTables   = $this->getDBManager()->listTables();
        $entityTables  = $this->getTableInfoFromClasses();
        $compareResult = $this->compareTableSet($mongoTables, $entityTables);

        if ($dryRun) {
            /** @var ComparableItem $cpItem */
            foreach ($compareResult as $cpItem) {
                $this->outputWrite($cpItem->toSql());
            }
        }
        else {
            /** @var ComparableItem $cpItem */
            foreach ($compareResult as $cpItem) {
                $this->outputWrite($cpItem->toSql());
                $cpItem->applyChanges($this->getDBManager()->getDatabase());
            }
            $this->outputWrite('Done.');
        }
    }

    protected function getTableInfoFromClasses()
    {
        $classes   = $this->getManagedItemClasses();
        $tableList = [];

        /**
         * @var  $class
         * @var ItemReflection $reflection
         */
        foreach ($classes as $class => $reflection) {
            $itemDef = $reflection->getItemDefinition();
            if ($itemDef->projected) {
                $this->outputWrite(sprintf("Class %s is projected class, will not create table.", $class));
                continue;
            }

            $table = new Table();
            // set name
            $table->setName($this->itemManager->getDefaultTablePrefix().$reflection->getTableName());

            // set primary index
            $primaryKeyColumns = [
                $reflection->getFieldNameByPropertyName($reflection->getItemDefinition()->primaryIndex->hash),
            ];

            if (!empty($reflection->getItemDefinition()->primaryIndex->range)) {
                $primaryKeyColumns[] = $reflection->getFieldNameByPropertyName(
                    $reflection->getItemDefinition()->primaryIndex->range
                );
            }

            $table->setPrimaryKeyColumns($primaryKeyColumns);

            // set other index
            foreach ($reflection->getItemDefinition()->globalSecondaryIndices as $globalSecondaryIndex) {
                $indexColumn   = [];
                $indexColumn[] = $reflection->getFieldNameByPropertyName($globalSecondaryIndex->hash);
                if (!empty($globalSecondaryIndex->range)) {
                    $indexColumn[] = $reflection->getFieldNameByPropertyName($globalSecondaryIndex->range);
                }
                $table->appendIndex(
                    (new Index())
                        ->setColumns($indexColumn)
                        ->setName($globalSecondaryIndex->name)
                );
            }

            // append to list
            $tableList[$table->getName()] = $table;
        }

        return $tableList;
    }

    protected function compareTableSet($tablesInDatabase, $tablesFromEntities)
    {
        $compareResult = [];

        /**
         * 1. find new tables and tables need to be changed
         *
         * @var string $name
         * @var Table $table
         */
        foreach ($tablesFromEntities as $name => $table) {
            if (!key_exists($name, $tablesInDatabase)) {
                $compareResult[] = $table->withChangeType(ComparableItem::IS_NEW);
                // create table index
                foreach ($table->getIndexs() as $index) {
                    $compareResult[] = $index->withChangeType(ComparableItem::IS_NEW);
                }
            }
            else {
                $tableCompareRet = $this->compareTable($table, $tablesInDatabase[$name]);
                $compareResult   = array_merge($compareResult, $tableCompareRet);
            }
        }

        /**
         * 1. find tables to be removed
         *
         * @var string $name2
         * @var Table $table2
         */
        foreach ($tablesInDatabase as $name2 => $table2) {
            if (!key_exists($name2, $tablesFromEntities)) {
                $compareResult[] = $table2->withChangeType(ComparableItem::TO_DELETE);
            }
        }

        return $compareResult;
    }

    protected function compareTable(Table $tableFromEntity, Table $tableInDatabase)
    {
        $result = [];

        // find new index
        foreach ($tableFromEntity->getIndexs() as $index) {
            $changeType = $tableInDatabase->compareIndex($index);
            if ($changeType !== ComparableItem::NO_CHANGE) {
                $result[] = $index->withChangeType($changeType);
            }
        }

        // find index to to delete
        foreach ($tableInDatabase->getIndexs() as $idx) {
            if ($tableFromEntity->hasIndex($idx) === false) {
                $result[] = $idx->withChangeType(ComparableItem::TO_DELETE);
            }
        }

        return $result;
    }
}
