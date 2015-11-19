<?php

namespace machour\yii2\elasticsearch\models;

use yii\helpers\Inflector;
use yii\helpers\StringHelper;

/**
 * Class ElasticRecord
 *
 * @package backend\modules\search\models
 */
abstract class ElasticRecord
{

    /**
     * @return \yii\db\ActiveRecord The ActiveRecord class related to this elasticsearch model
     */
    public abstract function getDBClass();

    /**
     * Gets the elasticsearch properties for this model
     *
     * @return array
     */
    public static function properties()
    {
        return [];
    }

    /**
     * @return string the name of the index this record is stored in.
     */
    public static function index()
    {
        return Inflector::pluralize(Inflector::camel2id(StringHelper::basename(get_called_class()), '-'));
    }

    /**
     * @return string the name of the type of this record.
     */
    public static function type()
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '-');
    }

    /**
     * @param int $limit
     * @return array
     */
    public static function getDbRecords($limit = 100000)
    {
        return static::select()->asArray()->limit($limit)->all();
    }

    /**
     * @param array $model
     * @return array
     */
    public function action($model)
    {
        return [
            'index' => [
                '_index' => static::index(),
                '_type' => static::type(),
                '_id' => $model['id'],
            ]
        ];
    }

    /**
     * @return array
     */
    public function select()
    {
        /** @var \yii\db\ActiveRecord $class */
        $class = static::getDBClass();
        return $class::find()->select(static::attributes());
    }


    /**
     * @param array $model
     * @return array The properties to be indexed
     */
    public function body($model)
    {
        return $model;
    }

    /**
     * Gets the ActiveRecord mapping definition
     *
     * @return array
     */
    public static function mapping()
    {
        return [
            'index' => self::index(),
            'type' => self::type(),
            'body' => [
                self::type() => [
                    '_source' => [
                        'enabled' => true
                    ],
                    'properties' => static::properties()
                ]
            ]
        ];
    }

    /**
     * Gets the query body for the elasticsearch request
     *
     * @param string $term The search string
     * @param array $filters The filters to be applied before querying
     * @return array
     */
    public static function query($term, $filters = [])
    {
        $attributes = array_keys(static::properties());

        $query = [
            'query' => [
                "filtered"  =>  [
                    "query"  =>  [
                        "flt" => [
                            "like_text" => $term,
                            "fields" => static::fltFields()
                        ],
                    ],
                ]
            ]
        ];

        foreach ($filters as $field => $value) {
            if (!in_array($field, $attributes)) continue;
            if (!isset($query['query']['filtered']['filter']['term'])) {
                $query['query']['filtered']['filter'] = ['term' => []];
            }
            $query['query']['filtered']['filter']['term'][$field] = $value;
        }

        return $query;
    }

    /**
     * Gets the list of fields that will be searched using `fuzzy_like_this`
     *
     * @return array The fields list
     */
    public static function fltFields()
    {
        $fields = [];
        foreach (static::properties() as $property => $info) {
            if ($info['type'] == 'string') {
                $fields[] = $property;
            }
        }
        return $fields;
    }

    /**
     * @inheritdoc
     */
    public function attributes()
    {
        return array_keys(static::properties());
    }
}