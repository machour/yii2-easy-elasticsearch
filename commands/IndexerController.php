<?php

namespace machour\yii2\elasticsearch\commands;

use machour\yii2\elasticsearch\models\ElasticRecord;
use Yii;
use yii\console\Controller;
use yii\helpers\Console;

/**
 * Elasticsearch console utility
 *
 * @package backend\modules\search\commands
 */
class IndexerController extends Controller
{
    /**
     * Displays the help message
     */
    public function actionIndex()
    {
        $this->run('/help', ['search/indexer']);
        $this->run('version');
    }

    /**
     * Indexes all registered models
     *
     * @param bool $purgeBefore Purge the index before indexing ?
     */
    public function actionIndexAll($purgeBefore = false)
    {
        foreach ($this->module->models as $model) {
            $this->run('index-model', [$model, $purgeBefore]);
            $this->stdout("\n");
        }
    }

    /**
     * Indexes a model
     *
     * @param ElasticRecord $model The elastic search model to be indexed
     * @param bool $purgeBefore Purge the index before indexing ?
     * @throws \yii\base\InvalidConfigException
     */
    public function actionIndexModel($model, $purgeBefore = false)
    {

        $this->stdout(sprintf("Indexing %s/%s ..\n", $model::index(), $model::type()));

        if ($purgeBefore) {
            $this->run('put-mapping', [
                $model,
                $purgeBefore
            ]);
        }

        /** @var ElasticRecord $instance */
        $instance = new $model();

        $batch = 1;

        $i = 0;

        $params = [];

        $total = 0;

        $batch_limit = 100;


        $records = $model::getDbRecords();

        Console::startProgress(0, count($records));


        foreach ($records as $m) {
            $i++;

            $params[] = $instance->action($m);
            $params[] = $instance->body($m);

            if ($i % $batch_limit == 0) {

                $responses = $this->module->client->bulk(['body' => $params]);
                $params = [];
                if (!$responses['errors']) {
                   // $this->stdout("Saved batch #$batch\n");
                    $total += count($responses['items']);
                } else {
                    //$this->stderr("Errors saving batch #$batch\n");
                }
                Console::updateProgress($batch * $batch_limit, count($records));

                $batch++;
            }
        }

        if (!empty($params)) {
            $responses = $this->module->client->bulk(['body' => $params]);
            if (!$responses['errors']) {
                //$this->stdout("Saved batch #$batch\n");
                $total += count($responses['items']);
            } else {
                //$this->stderr("Errors saving batch #$batch\n");
            }
            Console::updateProgress(count($records), count($records));

        }
        Console::endProgress();

        $this->stdout("Indexed $total {$model::index()}/{$model::type()} documents\n");
    }

    /**
     * Put the model's mapping into elasticsearch
     *
     * @param bool $purgeBefore Purge the index before putting the mapping ?
     * @param ElasticRecord $model The model name
     */
    public function actionPutMapping($model, $purgeBefore = true)
    {
        if ($purgeBefore) {
            if ($this->module->client->indices()->exists(['index' => $model::index()])) {
                $this->module->client->indices()->delete(['index' => $model::index()]);
            }
            $this->module->client->indices()->create(['index' => $model::index()]);
        }

        $result = $this->module->client->indices()->putMapping($model::mapping());

        if ($result['acknowledged']) {
            $this->stdout("$model mapping updated\n");
        } else {
            $this->stderr(var_export($result, 1));
        }

        $this->stdout("\n");
    }

    /**
     * Shows the server version information
     */
    public function actionVersion()
    {
        $info = $this->module->client->info();
        $this->stdout(sprintf("Elasticsearch version %s (lucene %s)\n",
            $info['version']['number'],
            $info['version']['lucene_version']
        ));
    }

    /**
     * Shows the indexes/types information
     */
    public function actionStatus()
    {
        $data = [];
        foreach ($this->module->client->indices()->getMapping() as $index => $info) {
            foreach ($info['mappings'] as $type => $props) {
                $count = $this->module->client->count(['index' => $index, 'type' => $type]);
                $data[] = [
                    'Index' => $index,
                    'Type' => $type,
                    'Count' => $count['count'],
                    'Shards' => $count['_shards']['successful'] . '/' . $count['_shards']['total'],
                ];
            }
        }
        $this->stdout($this->arrayToTable($data));
    }

    /**
     * Transforms a PHP array into an ascii table
     *
     * @link http://stackoverflow.com/questions/4505473/turn-any-array-into-a-text-table
     * @param $table
     * @return mixed
     */
    private function arrayToTable($table) {
        $cell_lengths = [];
        foreach ($table AS $row) {
            $cell_count = 0;
            foreach ($row AS $key => $cell) {
                $cell_length = strlen($cell);
                $key_length = strlen($key);
                $cell_length = $key_length > $cell_length ? $key_length : $cell_length;
                $cell_count++;
                if (!isset($cell_lengths[$key]) || $cell_length > $cell_lengths[$key]) {
                    $cell_lengths[$key] = $cell_length;
                }
            }
        }

        $bar = '+';
        $header = '|';

        foreach ($cell_lengths as $field => $length) {
            $bar .= str_pad('', $length+2, '-') . '+';
            $name = $field;
            if (strlen($name) > $length) {
                $name = substr($name, 0, $length-1);
            }
            $header .= ' '.str_pad($name, $length, ' ', STR_PAD_RIGHT) . ' |';
        }
        $output = "${bar}\n${header}\n${bar}\n";

        foreach ($table as $row) {
            $output .= '|';
            foreach ($row AS $key => $cell) {
                $output .= ' ' . str_pad($cell, $cell_lengths[$key], ' ', STR_PAD_RIGHT) . ' |';
            }
            $output .= "\n";
        }
        $output .= $bar."\n";
        return $output;
    }

}