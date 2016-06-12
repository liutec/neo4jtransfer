<?php

namespace Neo4jTransfer;

use Everyman\Neo4j\Client;
use Everyman\Neo4j\Cypher\Query;
use Symfony\Component\Console\Output\Output;
use Symfony\Component\Console\Output\OutputInterface;

class Neo4jTransfer
{
    const VERSION = '@version@';
    const RELEASE_DATE = '@release_date@';
    const SEP_SIZE = 60;

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
        $cqCountRelations = 'MATCH ()-[r]-() RETURN count(r), min(id(r)), max(id(r));';
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

    protected static function makeRelationCypher($type, $properties, $leftNodeId, $rightNodeId, $ignoreProperties=null)
    {
        $createCypher = sprintf(
            '_%d-[:`%s`%s]->_%d',
            $leftNodeId,
            static::escapeString($type),
            static::encodeProperties($properties, $ignoreProperties),
            $rightNodeId
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

    protected static function makeRelationCyphers($resultSet, $importLabel, $importIdKey, $ignoreProperties=null, $nodePrefix='', $nodeSuffix='')
    {
        $result = array('match' => array(), 'create' => array());
        $id = null;
        foreach ($resultSet as $row) {
            $id = $row[0]['metadata']['id'];
            $type = $row[0]['type'];
            $properties = $row[0]['data'];
            $leftNodeId = static::extractIdFromUrl($row[0]['start']);
            $rightNodeId = static::extractIdFromUrl($row[0]['end']);
            $matchLeft = sprintf('(_%d:`%s`{`%s`:%d})', $leftNodeId, $importLabel, $importIdKey, $leftNodeId);
            $matchRight = sprintf('(_%d:`%s`{`%s`:%d})', $rightNodeId, $importLabel, $importIdKey, $rightNodeId);
            $result['match'][$matchLeft] = $matchLeft;
            $result['match'][$matchRight] = $matchRight;
            $result['create'][] = $nodePrefix.static::makeRelationCypher($type, $properties, $leftNodeId, $rightNodeId, $ignoreProperties).$nodeSuffix;
        }
        return array($result, isset($id) ? $id + 1 : null);
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
        static::writelnInfo(sprintf('Source: %s@%s:%d', $source->getUsername(), $source->getHost(), $source->getPort()), $output, $stdout);
        static::sepInfo($sepSize, $output, $stdout);
        list($nodeCount, $minNodeId, $maxNodeId) = static::readNodeStats($client);
        static::writelnInfo(sprintf('Number of nodes:     %d [%d->%d]', $nodeCount, $minNodeId, $maxNodeId), $output, $stdout);
        list($relationCount, $minRelationId, $maxRelationId) = static::readRelationStats($client);
        static::writelnInfo(sprintf('Number of relations: %d [%d->%d]', $relationCount, $minRelationId, $maxRelationId), $output, $stdout);
        static::sepInfo($sepSize, $output, $stdout);
        fwrite($file, sprintf("//\n// CYPHER DUMP OF NEO4J DATABASE\n// FROM %s:%d TAKEN AT %s\n", $source->getHost(), $source->getPort(), date('Y-m-d H:i:s')));
        fwrite($file, sprintf("CREATE INDEX ON :`%s`(`%s`);\n", $importLabel, $importIdKey));
        if ($transactional) {
            fwrite($file, "// BEGIN TRANSACTION\n");
            fwrite($file, "BEGIN\n");
        }
        if ($clean) {
            fwrite($file, "// REMOVE NODES AND RELATIONS\n");
            fwrite($file, "MATCH ()-[r]-() DELETE r;\n");
            fwrite($file, "MATCH (n) DELETE n;\n");
        }
        if ($stdout) {
            static::writelnInfo('Nodes', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        } else {
            static::writelnInfo(sprintf('Dumping nodes:       %d batches', ceil($nodeCount / $nodeBatchSize)), $output, $stdout);
        }
        fwrite($file, sprintf("// %d NODES (first %d, last %d) IN %d BATCHES\n//\n\n", $nodeCount, $minNodeId, $maxNodeId, ceil($nodeCount / $nodeBatchSize)));
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
        fwrite($file, sprintf("//\n// %d RELATIONS (first %d, last %d) IN %d BATCHES\n//\n\n", $relationCount, $minRelationId, $maxRelationId, ceil($relationCount/$relationBatchSize)));
        if ($stdout) {
            static::writelnInfo('Relations', $output, $stdout);
            static::sepInfo($sepSize, $output, $stdout);
        } else {
            static::writelnInfo(sprintf('Dumping relations:   %d batches', ceil($relationCount/$relationBatchSize)), $output, $stdout);
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
        fwrite($file, "// REMOVE IMPORT LABEL AND PROPERTY FROM NODES\n");
        fwrite($file, sprintf("MATCH (n:`%s`) REMOVE n.`%s` SET n.`%s` = NULL;\n", $importLabel, $importLabel, $importIdKey));
        if ($transactional) {
            fwrite($file, "// COMMIT TRANSACTION\n");
            fwrite($file, "COMMIT\n");
        }
        fwrite($file, "// REMOVE IMPORT PROPERTY INDEX\n");
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
        static::writelnInfo(sprintf('Target: %s@%s:%d', $target->getUsername(), $target->getHost(), $target->getPort()), $output);
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
                }
                static::writelnInfo('>> '.substr($line, 2), $output);
                continue;
            }
            $cypher[] = $line;
            if (substr($line, -1)  === ';') {
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
}