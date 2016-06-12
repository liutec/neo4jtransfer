<?php

namespace Neo4jTransfer;

use Everyman\Neo4j\Batch;
use Everyman\Neo4j\Client;
use Everyman\Neo4j\Cypher\Query;
use Everyman\Neo4j\Label;
use Everyman\Neo4j\Node;
use Everyman\Neo4j\Relationship;
use Symfony\Component\Console\Output\Output;
use Symfony\Component\Console\Output\OutputInterface;

class Neo4jTransfer
{
    const VERSION = '@version@';
    const RELEASE_DATE = '@release_date@';
    const SEP_SIZE = 50;
    const CYPHER_REMOVE_ALL_RELATIONS = 'MATCH ()-[r]->() DELETE r;';
    const CYPHER_REMOVE_ALL_NODES = 'MATCH (n) DELETE n;';

    public static function getWithDefault($array, $key, $defaultValue = null)
    {
        return isset($array[$key]) ? $array[$key] : $defaultValue;
    }

    protected static function write($message, OutputInterface $output=null)
    {
        if (!isset($output)) {
            return;
        }
        $output->write($message);
    }

    protected static function writeln($message, OutputInterface $output=null)
    {
        if (!isset($output)) {
            return;
        }
        $output->writeln($message);
    }

    protected static function writeInfo($message, OutputInterface $output=null, $comment=false)
    {
        static::write($comment ? '// '.$message : $message, $output);
    }

    protected static function writelnInfo($message, OutputInterface $output=null, $comment=false)
    {
        static::writeln($comment ? '// '.$message : $message, $output);
    }

    protected static function sepInfo($size, OutputInterface $output=null, $comment=false)
    {
        static::writeln(($comment ? '// ' : '').str_repeat('=', $size), $output);
    }

    protected static function readNodeStats(Client $client)
    {
        $cqCountNodes = 'MATCH (n) RETURN count(n), min(id(n)), max(id(n));';
        $query = new Query($client, $cqCountNodes);
        $resultSet = $query->getResultSet();
        return $resultSet[0];
    }

    protected static function readRelationStats(Client $client)
    {
        $cqCountRelations = 'MATCH ()-[r]->() RETURN count(r), min(id(r)), max(id(r));';
        $query = new Query($client, $cqCountRelations);
        $resultSet = $query->getResultSet();
        return $resultSet[0];
    }

    protected static function readNodes(Client $client, $batchSize, $lastId, $fromId=0)
    {
        $cqDumpNodes = 'MATCH (n) WHERE ID(n) >= {fromId} RETURN n ORDER BY ID(n) ASC LIMIT {batchSize};';
        $query = new Query($client, $cqDumpNodes, array('fromId' => $fromId, 'batchSize' => $batchSize));
        $resultSet = $query->getResultSet();
        return static::accessProtected($resultSet, 'data');
    }

    protected static function readRelations(Client $client, $batchSize, $lastId, $fromId=0)
    {
        $cqDumpNodes = 'MATCH ()-[r]->() WHERE ID(r) >= {fromId} RETURN r ORDER BY ID(r) ASC LIMIT {batchSize};';
        $query = new Query($client, $cqDumpNodes, array('fromId' => $fromId, 'batchSize' => $batchSize));
        $resultSet = $query->getResultSet();
        return static::accessProtected($resultSet, 'data');
    }

    protected static function escapeString($value)
    {
        return str_replace(array('\\', '"', '&'), array('\\\\', '\"', '\u0026'), $value);
    }

    protected static function encodeString($value)
    {
        return '"'.static::escapeString($value).'"';
    }

    protected static function encodeValue($value)
    {
        switch(gettype($value)) {
            case null:
                return 'null';
            case 'boolean':
                return $value ? 'true' : 'false';
            case 'string':
                return static::encodeString($value);
            case 'array':
                if (empty($value)) {
                    return '[]';
                }
                $items = array();
                foreach ($value as &$item) {
                    $items[] = static::encodeValue($item);
                }
                return '['.implode(',', $items).']';
            default:
                return $value;
        }
    }

    protected static function encodeProperties($properties, $ignoreProperties=null)
    {
        if (empty($properties)) {
            return '';
        }
        $tProperties = array();
        foreach ($properties as $key => $value) {
            if ((!empty($ignoreProperties)) && in_array($key, $ignoreProperties)) {
                continue;
            }
            $tProperties[] = '`'.$key.'`:'.static::encodeValue($value);
        }
        if (empty($tProperties)) {
            return '';
        }
        return '{'.implode(',', $tProperties).'}';
    }

    protected static function encodeLabels($labels)
    {
        if (empty($labels)) {
            return '';
        }
        $tLabels = array();
        foreach ($labels as $label) {
            $tLabels[] = ':`'.static::escapeString($label).'`';
        }
        return implode('', $tLabels);
    }

    protected static function makeNodeCypher($id, $labels, $properties, $var=false)
    {
        $cId = $var ? '_'.$id : '';
        $createCypher = sprintf('(%s%s%s)', $cId, static::encodeLabels($labels), static::encodeProperties($properties));
        return $createCypher;
    }

    protected static function makeRelationCypher($type, $properties, $leftNodeVar, $rightNodeVar, $ignoreProperties=null)
    {
        $createCypher = sprintf(
            '%s-[:`%s`%s]->%s',
            $leftNodeVar,
            static::escapeString($type),
            static::encodeProperties($properties, $ignoreProperties),
            $rightNodeVar
        );
        return $createCypher;
    }

    protected static function accessProtected($obj, $prop) {
        $reflection = new \ReflectionClass($obj);
        $property = $reflection->getProperty($prop);
        $property->setAccessible(true);
        return $property->getValue($obj);
    }

    protected static function makeNodeCyphers($resultSet, $importLabel, $importIdKey, $nodePrefix='', $nodeSuffix='')
    {
        $result = array();
        $id = null;
        foreach ($resultSet as $row) {
            $id = $row[0]['metadata']['id'];
            $labels = $row[0]['metadata']['labels'];
            $labels[] = $importLabel;
            $properties = $row[0]['data'];
            if (isset($properties[$importIdKey])) {
                throw new \Exception(
                    sprintf(
                        'Node already has import key "%s": %s.',
                        $importIdKey,
                        json_encode($row)
                    )
                );
            }
            $properties[$importIdKey] = $id;
            $result[] = $nodePrefix.static::makeNodeCypher($id, $labels, $properties).$nodeSuffix;
        }
        return array($result, isset($id) ? $id + 1 : null);
    }

    protected static function extractIdFromUrl($url)
    {
        $id = substr($url, strrpos($url, '/')+1);
        return intval($id);
    }

    protected static function encodeNodeVar($id)
    {
        return '_'.$id;
    }

    protected static function getNodeVar($nodeId, &$nodeVars, &$lastNodeVar)
    {
        if (!isset($nodeVars[$nodeId])) {
            $nodeVars[$nodeId] = $lastNodeVar;
            $lastNodeVar++;
        }
        return static::encodeNodeVar($nodeVars[$nodeId]);
    }

    protected static function makeRelationCyphers($resultSet, $importLabel, $importIdKey, $ignoreProperties=null, $nodePrefix='', $nodeSuffix='')
    {
        $result = array('match' => array(), 'create' => array());
        $id = null;
        $lastNodeVar = 0;
        $nodeVars = array();
        foreach ($resultSet as $row) {
            $id = $row[0]['metadata']['id'];
            $type = $row[0]['type'];
            $properties = $row[0]['data'];
            $leftNodeId = static::extractIdFromUrl($row[0]['start']);
            $rightNodeId = static::extractIdFromUrl($row[0]['end']);
            $leftNodeVar = static::getNodeVar($leftNodeId, $nodeVars, $lastNodeVar);
            $rightNodeVar = static::getNodeVar($rightNodeId, $nodeVars, $lastNodeVar);
            $matchLeft = sprintf('(%s:`%s`{`%s`:%d})', $leftNodeVar, $importLabel, $importIdKey, $leftNodeId);
            $matchRight = sprintf('(%s:`%s`{`%s`:%d})', $rightNodeVar, $importLabel, $importIdKey, $rightNodeId);
            $result['match'][$matchLeft] = $matchLeft;
            $result['match'][$matchRight] = $matchRight;
            $result['create'][] = $nodePrefix.static::makeRelationCypher($type, $properties, $leftNodeVar, $rightNodeVar, $ignoreProperties).$nodeSuffix;
        }
        return array($result, isset($id) ? $id + 1 : null);
    }

    protected static function getRemoveAllRelationsCypher()
    {
        return '';
    }

    public static function dump(Neo4jConnection $source, $importLabel, $importIdKey, $readBatchSize,
                                           $nodeBatchSize, $relationBatchSize, $clean=true, $transactional=false,
                                           $ignoredRelationProperties=null, $file=null, OutputInterface $output=null)
    {
        if (isset($file)) {
            $stdout = false;
        } else {
            $stdout = true;
            $file = STDOUT;
        }
        $sepSize = self::SEP_SIZE;
        $client = $source->makeClient();
        static::writelnInfo(sprintf('Reading from:        %s:%d', $source->getHost(), $source->getPort()), $output, $stdout);
        static::writelnInfo(sprintf('Read batch size:     %d', $readBatchSize), $output, $stdout);
        static::sepInfo($sepSize, $output, $stdout);
        list($nodeCount, $minNodeId, $maxNodeId) = static::readNodeStats($client);
        static::writelnInfo(sprintf('Number of nodes:     %d [%d->%d]', $nodeCount, $minNodeId, $maxNodeId), $output, $stdout);
        list($relationCount, $minRelationId, $maxRelationId) = static::readRelationStats($client);
        static::writelnInfo(sprintf('Number of relations: %d [%d->%d]', $relationCount, $minRelationId, $maxRelationId), $output, $stdout);
        static::sepInfo($sepSize, $output, $stdout);
        fwrite($file, sprintf("//\n// CYPHER DUMP OF NEO4J DATABASE\n// host: %s:%d\n// time: %s\n//\n// https://github.com/liutec/neo4jtransfer\n//\n", $source->getHost(), $source->getPort(), date('Y-m-d H:i:s')));
        fwrite($file, "\n// CREATE IMPORT PROPERTY INDEX\n");
        fwrite($file, sprintf("CREATE INDEX ON :`%s`(`%s`);\n", $importLabel, $importIdKey));
        if ($transactional) {
            fwrite($file, "\n// BEGIN TRANSACTION\n");
            fwrite($file, "BEGIN\n");
        }
        if ($clean) {
            fwrite($file, "\n// REMOVE ALL RELATIONS AND NODES\n");
            fwrite($file, self::CYPHER_REMOVE_ALL_RELATIONS."\n");
            fwrite($file, self::CYPHER_REMOVE_ALL_NODES."\n");
        }
        if ($stdout) {
            static::writelnInfo('Nodes', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        } else {
            static::writelnInfo(sprintf('Dumping nodes:       %d batches of %d', ceil($nodeCount / $nodeBatchSize), $nodeBatchSize), $output, $stdout);
        }
        fwrite($file, sprintf("\n// %d NODES IN %d BATCHES OF %d\n\n", $nodeCount, ceil($nodeCount / $nodeBatchSize), $nodeBatchSize));
        $fromNodeId = 0;
        $k = 0;
        while (isset($fromNodeId)) {
            if (!$stdout) {
                static::writeInfo('*', $output, $stdout);
                $k++;
                if ($k >= $sepSize) {
                    $k = 0;
                    static::writelnInfo('', $output, $stdout);
                }
            }
            $resultSet = static::readNodes($client, $readBatchSize, $maxNodeId, $fromNodeId);
            if (empty($resultSet)) {
                break;
            }
            $i = 0;
            while ($i < count($resultSet)) {
                $batch = array_slice($resultSet, $i, $nodeBatchSize);
                list($nodes, $fromNodeId) = static::makeNodeCyphers($batch, $importLabel, $importIdKey);
                fwrite($file, 'CREATE '.implode(',', $nodes).";\n");
                if (!$stdout) {
                    static::writeInfo('-', $output, $stdout);
                    $k++;
                    if ($k >= $sepSize) {
                        $k = 0;
                        static::writelnInfo('', $output, $stdout);
                    }
                }
                $i += $nodeBatchSize;
            }
        }
        if (!$stdout) {
            static::writelnInfo(' .', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        }
        fflush($file);
        fwrite($file, sprintf("\n// %d RELATIONS IN %d BATCHES OF %d\n\n", $relationCount, ceil($relationCount/$relationBatchSize), $relationBatchSize));
        if ($stdout) {
            static::writelnInfo('Relations', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        } else {
            static::writelnInfo(sprintf('Dumping relations:   %d batches of %d', ceil($relationCount/$relationBatchSize), $relationBatchSize), $output, $stdout);
        }
        $fromRelationId = 0;
        $k = 0;
        while (isset($fromRelationId)) {
            if (!$stdout) {
                static::writeInfo('*', $output, $stdout);
                $k++;
                if ($k >= $sepSize) {
                    $k = 0;
                    static::writelnInfo('', $output, $stdout);
                }
            }
            $resultSet = static::readRelations($client, $readBatchSize, $maxRelationId, $fromRelationId);
            if (empty($resultSet)) {
                break;
            }
            $i = 0;
            while ($i < count($resultSet)) {
                $batch = array_slice($resultSet, $i, $relationBatchSize);
                list($relations, $fromRelationId) = static::makeRelationCyphers($batch, $importLabel, $importIdKey, $ignoredRelationProperties);
                if (empty($relations['create'])) {
                    break;
                }
                fwrite($file, 'MATCH '.implode(',', $relations['match']).' CREATE '.implode(',', $relations['create']).";\n");
                if (!$stdout) {
                    static::writeInfo('-', $output, $stdout);
                    $k++;
                    if ($k >= $sepSize) {
                        $k = 0;
                        static::writelnInfo('', $output, $stdout);
                    }
                }
                $i += $relationBatchSize;
            }
        }
        fflush($file);
        if (!$stdout) {
            static::writelnInfo(' .', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        }
        fwrite($file, "\n// REMOVE IMPORT LABEL AND PROPERTY FROM NODES\n");
        fwrite($file, sprintf("MATCH (n:`%s`) REMOVE n.`%s` SET n.`%s` = NULL;\n", $importLabel, $importLabel, $importIdKey));
        if ($transactional) {
            fwrite($file, "\n// COMMIT TRANSACTION\n");
            fwrite($file, "COMMIT\n");
        }
        fwrite($file, "\n// REMOVE IMPORT PROPERTY INDEX\n");
        fwrite($file, sprintf("DROP INDEX ON :`%s`(`%s`);\n", $importLabel, $importIdKey));
        fflush($file);
    }

    public static function import(Neo4jConnection $target, $file=null, OutputInterface $output=null)
    {
        if (isset($file)) {
            $stdin = false;
        } else {
            $stdin = true;
            $file = STDIN;
        }
        $client = $target->makeClient();
        $cypher = array();
        $sepSize = self::SEP_SIZE;
        $k = 0;
        $needBreak = false;
        $prevComment = false;
        static::writelnInfo(sprintf('Target: %s:%d', $target->getHost(), $target->getPort()), $output);
        static::sepInfo($sepSize, $output);
        while (($line = fgets($file)) !== false) {
            $line = trim($line);
            if (empty($line)) {
                continue;
            }
            if (substr($line, 0, 2) == '//') {
                if ($needBreak) {
                    static::writelnInfo('', $output);
                    $needBreak = false;
                    $k = 0;
                    if (!$prevComment) {
                        static::sepInfo($sepSize, $output);
                    }
                }
                static::writelnInfo('>> '.substr($line, 2), $output);
                $prevComment = true;
                continue;
            }
            $cypher[] = $line;
            if (substr($line, -1)  === ';') {
                if ($prevComment) {
                    static::sepInfo($sepSize, $output);
                }
                $query = new Query($client, implode(' ', $cypher));
                $query->getResultSet();
                $cypher = array();
                static::writeInfo('.', $output);
                $needBreak = true;
                $k++;
                if ($k >= $sepSize) {
                    $k = 0;
                    static::writelnInfo('', $output);
                }
            }
            $prevComment = false;
        }
        if ($needBreak) {
            static::writelnInfo('', $output);
            $needBreak = false;
        }
        static::sepInfo($sepSize, $output);
    }
    public static function dumpAndImport(Neo4jConnection $source, Neo4jConnection $target, $importLabel, $importIdKey,
                                         $readBatchSize, $nodeBatchSize, $relationBatchSize, $clean, $transactional,
                                         $ignoredRelationProperties=null, $file=null, OutputInterface $output=null)
    {
        
    }
    
    public static function transfer(Neo4jConnection $source, Neo4jConnection $target, $importLabel, $importIdKey,
                                    $readBatchSize, $nodeBatchSize, $relationBatchSize, $clean, $transactional,
                                    $ignoredRelationProperties=null, $file=null, OutputInterface $output=null)
    {
        static::dump(
            $source,
            $importLabel,
            $importIdKey,
            $readBatchSize,
            $nodeBatchSize,
            $relationBatchSize,
            $clean,
            $transactional,
            $ignoredRelationProperties,
            $file,
            $output
        );
        fseek($file, 0);
        static::import(
            $target,
            $file,
            $output
        );
    }

    protected static function findLabelsByNames($names, $labels)
    {
        $result = array();
        foreach ($names as $name) {
            if (isset($labels[$name])) {
                $result[$name] = $labels[$name];
            }
        }
        return $result;
    }

    protected static function importNodes(&$nodeIds, Client $targetClient, $resultSet)
    {
        $result = array();
        $id = null;
        $batch = array();
        $returnIds = array();
        foreach ($resultSet as $row) {
            $id = $row[0]['metadata']['id'];
            $labels = $row[0]['metadata']['labels'];
            $properties = $row[0]['data'];
            $colName = 'ID(_'.$id.')';
            $returnIds[$colName] = $id;
            $batch[] = static::makeNodeCypher($id, $labels, $properties, true);
        }
        $cypher = 'CREATE '.implode(',', $batch).' RETURN '.implode(',', array_keys($returnIds)).';';
        $query = new Query($targetClient, $cypher);
        $resultSet = $query->getResultSet();
        $columns = static::accessProtected($resultSet, 'columns');
        $data = static::accessProtected($resultSet, 'data');
        foreach ($columns as $colIdx => $colName) {
            $nodeIds[$returnIds[$colName]] = $data[0][$colIdx];
        }
        return array($result, isset($id) ? $id + 1 : null);
    }

    protected static function importRelations($nodeIds, Client $targetClient, $resultSet, $ignoreProperties=null)
    {
        $result = array();
        $id = null;
        $batch = new Batch($targetClient);
        foreach ($resultSet as $row) {
            $id = $row[0]['metadata']['id'];
            $type = $row[0]['type'];
            $properties = $row[0]['data'];
            if (isset($ignoreProperties)) {
                foreach ($properties as $key => $propertyName) {
                    if (in_array($propertyName, $ignoreProperties)) {
                        unset($properties[$key]);
                    }
                }
            }
            $leftNode = $targetClient->makeNode();
            $leftNodeId = $nodeIds[static::extractIdFromUrl($row[0]['start'])];
            $leftNode->setId($leftNodeId);

            $rightNode = $targetClient->makeNode();
            $rightNodeId = $nodeIds[static::extractIdFromUrl($row[0]['end'])];
            $rightNode->setId($rightNodeId);

            $r = $targetClient->makeRelationship($properties);
            $r
                ->setStartNode($leftNode)
                ->setEndNode($rightNode)
                ->setType($type)
                ->setProperties($properties)
            ;
            $batch->save($r);
        }
        $batch->commit();
        return array($result, isset($id) ? $id + 1 : null);
    }

    protected static function getLabelsByName(Client $client)
    {
        $labelsByName = array();
        /** @var Label[] $labels */
        $labels = $client->getLabels();
        foreach ($labels as $label) {
            $labelsByName[$label->getName()] = $label;
        }
        return $labelsByName;
    }

    public static function directTransfer(Neo4jConnection $source, Neo4jConnection $target, $readBatchSize,
                                          $nodeBatchSize, $relationBatchSize, $ignoredRelationProperties=null,
                                          OutputInterface $output=null)
    {
        $sepSize = self::SEP_SIZE;

        $sourceClient = $source->makeClient();

        static::writelnInfo(sprintf('Reading from:        %s:%d', $source->getHost(), $source->getPort()), $output);
        static::sepInfo($sepSize, $output);
        static::writelnInfo(sprintf('Read batch size:     %d', $readBatchSize), $output);
        list($nodeCount, $minNodeId, $maxNodeId) = static::readNodeStats($sourceClient);
        static::writelnInfo(sprintf('Number of nodes:     %d [%d->%d]', $nodeCount, $minNodeId, $maxNodeId), $output);
        list($relationCount, $minRelationId, $maxRelationId) = static::readRelationStats($sourceClient);
        static::writelnInfo(sprintf('Number of relations: %d [%d->%d]', $relationCount, $minRelationId, $maxRelationId), $output);
        static::sepInfo($sepSize, $output);

        $targetClient = $target->makeClient();
        static::writelnInfo(sprintf('Writing to:          %s:%d', $target->getHost(), $target->getPort()), $output);
        static::sepInfo($sepSize, $output);
        static::writelnInfo(sprintf('Node batch size:     %d', $nodeBatchSize), $output);
        static::writelnInfo(sprintf('Relation batch size: %d', $relationBatchSize), $output);
        static::sepInfo($sepSize, $output);
        list($targetRelationCount, $targetMinRelationId, $targetMaxRelationId) = static::readRelationStats($targetClient);
        static::writelnInfo(sprintf('Removing relations:  %d [%d->%d]', $targetRelationCount, $targetMinRelationId, $targetMaxRelationId), $output);
        $targetClient->executeCypherQuery(new Query($targetClient, self::CYPHER_REMOVE_ALL_RELATIONS));
        static::sepInfo($sepSize, $output);
        list($targetNodeCount, $targetMinNodeId, $targetMaxNodeId) = static::readNodeStats($targetClient);
        static::writelnInfo(sprintf('Removing nodes:      %d [%d->%d]', $targetNodeCount, $targetMinNodeId, $targetMaxNodeId), $output);
        $targetClient->executeCypherQuery(new Query($targetClient, self::CYPHER_REMOVE_ALL_NODES));
        static::sepInfo($sepSize, $output);

        $nodeIds = array();

        static::writelnInfo(sprintf('Node transfer:       %d (%d batches of %d)', $nodeCount, ceil($nodeCount / $nodeBatchSize), $nodeBatchSize), $output);
        static::sepInfo($sepSize, $output);
        $fromNodeId = 0;
        $k = 0;
        while (isset($fromNodeId)) {
            static::writeInfo('*', $output);
            $k++;
            if ($k >= $sepSize) {
                $k = 0;
                static::writelnInfo('', $output);
            }
            $resultSet = static::readNodes($sourceClient, $readBatchSize, $maxNodeId, $fromNodeId);
            if (empty($resultSet)) {
                break;
            }
            $i = 0;
            while ($i < count($resultSet)) {
                $batch = array_slice($resultSet, $i, $nodeBatchSize);
                list($nodes, $fromNodeId) = static::importNodes($nodeIds, $targetClient, $batch);
                static::writeInfo('-', $output);
                $k++;
                if ($k >= $sepSize) {
                    $k = 0;
                    static::writelnInfo('', $output);
                }
                $i += $nodeBatchSize;
            }
        }
        static::writelnInfo(' .', $output);
        static::sepInfo($sepSize, $output);

        static::writelnInfo(sprintf('Relation transfer:   %d (%d batches of %d)', $relationCount, ceil($relationCount / $relationBatchSize), $relationBatchSize), $output);
        static::sepInfo($sepSize, $output);
        $fromRelationId = 0;
        $k = 0;
        while (isset($fromRelationId)) {
            static::writeInfo('*', $output);
            $k++;
            if ($k >= $sepSize) {
                $k = 0;
                static::writelnInfo('', $output);
            }
            $resultSet = static::readRelations($sourceClient, $readBatchSize, $maxRelationId, $fromRelationId);
            if (empty($resultSet)) {
                break;
            }
            $i = 0;
            while ($i < count($resultSet)) {
                $batch = array_slice($resultSet, $i, $relationBatchSize);
                list($relations, $fromRelationId) = static::importRelations($nodeIds, $targetClient, $batch, $ignoredRelationProperties);
                static::writeInfo('-', $output);
                $k++;
                if ($k >= $sepSize) {
                    $k = 0;
                    static::writelnInfo('', $output);
                }
                $i += $relationBatchSize;
            }
        }
        static::writelnInfo(' .', $output);
        static::sepInfo($sepSize, $output);
    }
}