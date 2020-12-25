<?php


namespace Oasis\Mlib\ODM\MongoDB\Schema\Structures;

use MongoDB\Database;
use Oasis\Mlib\Utils\Exceptions\DataValidationException;

/**
 * Class Index
 * @package Oasis\Mlib\ODM\Spanner\Schema\Structures
 */
class Index extends ComparableItem
{
    /**
     * @var string
     */
    private $name = '';

    /**
     * @var string
     */
    private $tableName = '';

    /**
     * @var array
     */
    private $columns = [];

    /**
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * @param  string  $tableName
     */
    public function setTableName($tableName)
    {
        $this->tableName = $tableName;
    }

    public function __toArray()
    {
        return [
            'name'    => $this->getName(),
            'table'   => $this->tableName,
            'columns' => $this->columns,
        ];
    }

    /**
     * @return string
     */
    public function getName()
    {
        if (empty($this->name) && !empty($this->columns)) {
            $idxName = '';
            foreach ($this->columns as $col) {
                $idxName .= strtolower($col).'_';
            }

            return rtrim($idxName, '_');
        }

        return $this->name;
    }

    /**
     * @param  string  $name
     * @return Index
     */
    public function setName($name)
    {
        $this->name = str_replace("-", '_', $name);

        return $this;
    }

    public function toSql()
    {
        if (empty($this->tableName)) {
            return '';
        }
        if ($this->changeType === self::NO_CHANGE) {
            return '';
        }

        $indexName = $this->getName();
        $columnSql = implode(',', $this->columns);

        switch ($this->changeType) {
            case self::IS_NEW:
                return "Will create index {$indexName} ON {$this->tableName} ({$columnSql})";
            case self::IS_MODIFIED:
                return "Notice: change index is unsupported";
            case self::TO_DELETE:
                return "Will drop INDEX {$indexName} from table {$this->tableName}";
            default:
                return '';
        }
    }

    public function applyChanges(Database $database)
    {
        if ($this->changeType === self::NO_CHANGE) {
            return true;
        }

        $keys = [];
        foreach ($this->getColumns() as $column) {
            $keys[$column] = 1;
        }

        switch ($this->changeType) {
            case self::IS_NEW:
                $database->selectCollection($this->tableName)->createIndex($keys, ['name' => $this->getName()]);
                break;
            case self::IS_MODIFIED:
                // nothing to do
                break;
            case self::TO_DELETE:
                $database->selectCollection($this->tableName)->dropIndex($this->getName());
                break;
            default:
                throw new DataValidationException("Error: unknown change type: {$this->changeType}");
        }

        return true;
    }

    /**
     * @return array
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * @param  array  $columns
     * @return Index
     */
    public function setColumns($columns)
    {
        $this->columns = $columns;

        return $this;
    }
}
