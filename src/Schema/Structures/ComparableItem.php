<?php

namespace Oasis\Mlib\ODM\MongoDB\Schema\Structures;

use MongoDB\Database;

abstract class ComparableItem
{
    public const NO_CHANGE   = 0;
    public const IS_NEW      = 1;
    public const IS_MODIFIED = 2;
    public const TO_DELETE   = 3;

    /**
     * @var int
     */
    protected $changeType = 0;

    /**
     * @return int
     */
    public function getChangeType()
    {
        return $this->changeType;
    }

    /**
     * @param  int  $changeType
     * @return ComparableItem
     */
    public function withChangeType($changeType)
    {
        $this->changeType = $changeType;

        return $this;
    }

    abstract public function toSql();

    abstract public function applyChanges(Database $database);

}
