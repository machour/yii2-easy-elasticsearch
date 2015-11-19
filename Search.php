<?php

namespace machour\yii2\elasticsearch;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use machour\yii2\elasticsearch\models\ElasticRecord;
use Yii;
use yii\base\Module;

/**
 * Class Search
 *
 * @package backend\modules\search
 */
class Search extends Module
{
    /**
     * @var Client
     */
    public $client;

    /**
     * @var string
     */
    public $controllerNamespace = 'machour\yii2\elasticsearch\commands';

    /**
     * @var ElasticRecord[]
     */
    public $models;

    /**
     * @inheritdoc
     */
    public function init()
    {
        $this->client = ClientBuilder::create()->build();
        parent::init();
    }

    /**
     * Performs the elasticsearch request
     *
     * @param string $term The search string
     * @param array $filters The filters to be applied before querying
     * @return array The search response from elasticsearch
     */
    public function search($term, $filters = [])
    {
        $body = [];
        foreach ($this->models as $model) {
            $body[] = ['index' => $model::index()];
            $body[] = $model::query($term, $filters);
        }
        return $this->client->msearch(['body' => $body]);
    }


}

