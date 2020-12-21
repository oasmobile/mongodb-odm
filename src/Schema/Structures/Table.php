<?php


namespace Oasis\Mlib\ODM\MongoDB\Schema\Structures;

use MongoDB\Database;
use Oasis\Mlib\Utils\Exceptions\DataValidationException;

/**
 * Class Table
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Table extends ComparableItem
{
    /**
     * @var string
     */
    private $name = '';

    /**
     * @var Index[]
     */
    private $indexs = [];

    /**
     * @var array
     */
    private $primaryKeyColumns = [];

    /**
     * @var array
     */
    private $indexMap = [];

    /**
     * @param  array  $primaryKeyColumns
     */
    public function setPrimaryKeyColumns($primaryKeyColumns)
    {
        $this->primaryKeyColumns = $primaryKeyColumns;
        $this->appendIndex((new Index())->setName('primary_key')->setColumns($this->primaryKeyColumns));
    }

    /**
     * @param  Index  $index
     * @return Table
     */
    public function appendIndex(Index $index)
    {
        $index->setTableName($this->name);
        $this->indexs[] = $index;

        return $this;
    }

    /**
     * @return Index[]
     */
    public function getIndexs()
    {
        return $this->indexs;
    }

    /**
     * @param  Index[]  $indexs
     * @return Table
     */
    public function setIndexs($indexs)
    {
        $this->indexs = $indexs;

        return $this;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param  string  $name
     * @return Table
     */
    public function setName($name)
    {
        $this->name = $name;

        return $this;
    }

    /**
     * @return array
     */
    public function __toArray()
    {
        $table = [
            'name' => $this->name,
        ];

        foreach ($this->indexs as $index) {
            $table['indexs'][] = $index->__toArray();
        }

        return $table;
    }

    public function toSql()
    {
        if ($this->changeType === self::NO_CHANGE) {
            return '';
        }

        switch ($this->changeType) {
            case self::IS_NEW:
                return "Will create table: {$this->name}";
            case self::IS_MODIFIED:
                return "Notice: change table name is unsupported";
            case self::TO_DELETE:
                return "Will drop table: {$this->name}";
                break;
            default:
                return 'Error: unknown change type';
        }
    }

    public function applyChanges(Database $database)
    {
        if ($this->changeType === self::NO_CHANGE) {
            return true;
        }

        switch ($this->changeType) {
            case self::IS_NEW:
                // nothing to do here, table will be auto-created when create it's indexs
                break;
            case self::IS_MODIFIED:
                // nothing to do
                break;
            case self::TO_DELETE:
                $database->dropCollection($this->name);
                break;
            default:
                throw new DataValidationException("Error: unknown change type: {$this->changeType}");
        }

        return true;
    }

    public function compareIndex(Index $index)
    {
        foreach ($this->indexs as $idx) {
            if ($idx->getName() == $index->getName()) {
                return self::NO_CHANGE;
            }
        }

        return self::IS_NEW;
    }

    public function hasIndex(Index $index)
    {
        if (!empty($this->indexMap)) {
            return in_array($index->getName(), $this->indexMap);
        }

        foreach ($this->indexs as $idx) {
            $this->indexMap[] = $idx->getName();
        }

        return in_array($index->getName(), $this->indexMap);
    }

}
