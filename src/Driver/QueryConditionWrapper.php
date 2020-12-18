<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use Oasis\Mlib\Utils\Exceptions\DataValidationException;

/**
 * Class QueryConditionWrapper
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class QueryConditionWrapper
{
    public const AND     = '&&';
    public const BETWEEN = '^';
    //
    protected $filter = [];


    /**
     * QueryConditionWrapper constructor.
     * @param $keyConditions
     * @param  array  $fieldsMapping
     * @param  array  $paramsMapping
     */
    public function __construct($keyConditions, array $fieldsMapping, array $paramsMapping)
    {
        $this->filter = $this->createFilterFromQueryConditions(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping
        );
    }

    public function getFilter()
    {
        if (empty($this->filter)) {
            throw new DataValidationException("Query condition is empty");
        }

        return $this->filter;
    }


    protected function createFilterFromQueryConditions($keyConditions, array $fieldsMapping, array $paramsMapping)
    {
        $filter          = [];
        $subQueryStrings = $this->explodeKeyConditions($keyConditions, $fieldsMapping, $paramsMapping);

        foreach ($subQueryStrings as $queryString) {
            /**
             * item like:
             *  ['name' => 'joh']
             *  ['age'  => ['$gt' =>15]]
             */
            $fEle = $this->transQueryStringToFilterElement($queryString);
            foreach ($fEle as $key => $val) {
                $filter[$key] = $val;
            }
        }

        return $filter;
    }

    protected function explodeKeyConditions($keyConditions, array $fieldsMapping, array $paramsMapping)
    {
        $keyConditions = $this->normalizeOperatorInQuery($keyConditions);
        $keyConditions = $this->fulfillQueryString($keyConditions, $fieldsMapping, $paramsMapping);
        $inx           = strpos($keyConditions, self:: AND);
        $ret           = [];
        if ($inx !== false) {
            $ret[] = trim(substr($keyConditions, 0, $inx));
            $ret[] = trim(substr($keyConditions, $inx + strlen(self:: AND)));
        }
        else {
            $ret[] = $keyConditions;
        }

        return $ret;
    }

    protected function fulfillQueryString($keyConditions, array $fieldsMapping, array $paramsMapping)
    {
        $replaceSearch  = array_keys($fieldsMapping);
        $replaceReplace = array_values($fieldsMapping);
        $replaceSearch  = array_merge($replaceSearch, array_keys($paramsMapping));
        $replaceReplace = array_merge($replaceReplace, array_values($paramsMapping));

        return str_replace($replaceSearch, $replaceReplace, $keyConditions);
    }

    protected function normalizeOperatorInQuery($str)
    {
        return str_ireplace(
            [
                'and',
                'between',
            ],
            [
                self:: AND,
                self::BETWEEN,
            ],
            $str
        );
    }

    protected function transQueryStringToFilterElement($queryString)
    {
        if (strpos($queryString, '=') !== false) {
            return $this->getCompareExpression($queryString, '=');
        }
        if (strpos($queryString, '>') !== false) {
            return $this->getCompareExpression($queryString, '>');
        }
        if (strpos($queryString, '>=') !== false) {
            return $this->getCompareExpression($queryString, '>=');
        }
        if (strpos($queryString, '<') !== false) {
            return $this->getCompareExpression($queryString, '<');
        }
        if (strpos($queryString, '<=') !== false) {
            return $this->getCompareExpression($queryString, '<=');
        }
        if (strpos($queryString, self::BETWEEN) !== false) {
            return $this->getBetweenExpression($queryString);
        }

        throw new DataValidationException("Unrecognized comparison query operators");
    }

    protected function getCompareExpression($queryString, $operator)
    {
        $arr = explode($operator, $queryString);

        switch ($operator) {
            case '=':
                $mongoOperator = '$eq';
                break;
            case '>':
                $mongoOperator = '$gt';
                break;
            case '>=':
                $mongoOperator = '$gte';
                break;
            case '<':
                $mongoOperator = '$lt';
                break;
            case '<=':
                $mongoOperator = '$lte';
                break;
            default:
                throw new DataValidationException("Unknown operator: $operator");
        }

        return [
            trim($arr[0]) => [
                $mongoOperator => trim($arr[1]),
            ],
        ];
    }

    protected function getBetweenExpression($queryString)
    {
        $arr = explode(self::BETWEEN, $queryString);
        if (count($arr) !== 2) {
            throw new DataValidationException("Invalid between compare expression: $queryString");
        }
        $val = explode(self:: AND, $arr[1]);
        if (count($val) !== 2) {
            throw new DataValidationException("Invalid between compare expression: $queryString");
        }

        return [
            trim($arr[0]) => [
                '$gte' => trim($val[0]),
                '$lte' => trim($val[1]),
            ],
        ];
    }


}
