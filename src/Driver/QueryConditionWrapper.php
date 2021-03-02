<?php

namespace Oasis\Mlib\ODM\MongoDB\Driver;

use Oasis\Mlib\Utils\Exceptions\DataValidationException;

/**
 * Class QueryConditionWrapper
 * @package Oasis\Mlib\ODM\MongoDB\Driver
 */
class QueryConditionWrapper
{
    public const AND     = ':&&';
    public const BETWEEN = ':^';
    public const EQUAL   = ':=';
    public const GT      = ':>';
    public const LT      = ':<';
    public const ELT     = ':>=';
    public const EGT     = ':<=';
    //
    protected $filter         = [];
    protected $attributeTypes = [];

    /**
     * QueryConditionWrapper constructor.
     * @param $keyConditions
     * @param  array  $fieldsMapping
     * @param  array  $paramsMapping
     * @param  array  $attributeTypes
     */
    public function __construct($keyConditions, array $fieldsMapping, array $paramsMapping, array $attributeTypes)
    {
        $this->attributeTypes = $attributeTypes;
        $this->filter         = $this->createFilterFromQueryConditions(
            $keyConditions,
            $fieldsMapping,
            $paramsMapping
        );
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
        if (empty($keyConditions)) {
            return [];
        }

        $keyConditions = $this->normalizeOperatorInQuery($keyConditions);
        $keyConditions = $this->fulfillQueryString($keyConditions, $fieldsMapping, $paramsMapping);

        /**
         * - In ODM there is at most 2 attributes in index: hash-key, sort-key
         * - The only logical operator in ODM is : AND
         */
        $inx = strpos($keyConditions, self:: AND);
        $ret = [];
        if ($inx !== false) {
            $ret[] = trim(substr($keyConditions, 0, $inx));
            $ret[] = trim(substr($keyConditions, $inx + strlen(self:: AND)));
        }
        else {
            $ret[] = $keyConditions;
        }

        return $ret;
    }

    protected function normalizeOperatorInQuery($str)
    {
        return str_ireplace(
            [
                'and',
                'between',
                '=',
                '>',
                '>=',
                '<',
                '<=',
            ],
            [
                self:: AND,
                self::BETWEEN,
                self::EQUAL,
                self::GT,
                self::EGT,
                self::LT,
                self::ELT,
            ],
            $str
        );
    }

    protected function fulfillQueryString($keyConditions, array $fieldsMapping, array $paramsMapping)
    {
        $replaceSearch  = array_keys($fieldsMapping);
        $replaceReplace = array_values($fieldsMapping);
        $replaceSearch  = array_merge($replaceSearch, array_keys($paramsMapping));
        $replaceReplace = array_merge($replaceReplace, array_values($paramsMapping));

        return str_replace($replaceSearch, $replaceReplace, $keyConditions);
    }

    protected function transQueryStringToFilterElement($queryString)
    {
        if (strpos($queryString, self::EQUAL) !== false) {
            return $this->getCompareExpression($queryString, self::EQUAL);
        }
        if (strpos($queryString, self::GT) !== false) {
            return $this->getCompareExpression($queryString, self::GT);
        }
        if (strpos($queryString, self::EGT) !== false) {
            return $this->getCompareExpression($queryString, self::EGT);
        }
        if (strpos($queryString, self::LT) !== false) {
            return $this->getCompareExpression($queryString, self::LT);
        }
        if (strpos($queryString, self::ELT) !== false) {
            return $this->getCompareExpression($queryString, self::ELT);
        }
        if (strpos($queryString, self::BETWEEN) !== false) {
            return $this->getBetweenExpression($queryString);
        }

        throw new DataValidationException("Unrecognized comparison query operators");
    }

    protected function getCompareExpression($queryString, $operator)
    {
        $arr = explode($operator, $queryString);

        if (count($arr) !== 2) {
            throw new DataValidationException("Invalid compare expression: $queryString");
        }

        switch ($operator) {
            case self::EQUAL:
                $comparisonOperator = '$eq';
                break;
            case self::GT:
                $comparisonOperator = '$gt';
                break;
            case self::EGT:
                $comparisonOperator = '$gte';
                break;
            case self::LT:
                $comparisonOperator = '$lt';
                break;
            case self::ELT:
                $comparisonOperator = '$lte';
                break;
            default:
                throw new DataValidationException("Unknown operator: $operator");
        }

        $att = trim($arr[0]);

        return [
            $att => [
                $comparisonOperator => $this->getTypedValue($arr[1], $att),
            ],
        ];
    }

    protected function getTypedValue($val, $attribute)
    {
        $val  = trim($val);
        $type = $this->attributeTypes[$attribute];
        if (empty($type)) {
            throw new DataValidationException("Unknown attribute: $attribute");
        }

        $getNumberValue = function ($val) {
            if (strpos($val, '.') === false) {
                return intval($val);
            }

            return floatval($val);
        };

        switch ($type) {
            case 'number':
                return $getNumberValue($val);
            default:
                return $val;
        }
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

        $att = trim($arr[0]);

        return [
            $att => [
                '$gte' => $this->getTypedValue($val[0], $att),
                '$lte' => $this->getTypedValue($val[1], $att),
            ],
        ];
    }

    public function getFilter()
    {
        if (empty($this->filter)) {
            $this->filter = [];
        }

        return $this->filter;
    }


}
