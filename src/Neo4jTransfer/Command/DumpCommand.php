<?php

namespace Neo4jTransfer\Command;

use Neo4jTransfer\Neo4jConnection;
use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class DumpCommand extends BaseCommand
{
    protected function configure()
    {
        static::configureDump($this);
    }

    public static function configureDump(BaseCommand $command, $name='dump')
    {
        $command
            ->setName($name)
            ->setDescription('Dump all nodes and relations from a Neo4j Server database.')
        ;
        static::configureSourceConnectionOptions($command);
        static::configureOutputOptions($command);
    }

    public static function configureSourceConnectionOptions(BaseCommand $command)
    {
        $command->addOption('source-host', null, InputArgument::OPTIONAL, 'Neo4j source server hostname.');
        $command->addOption('source-port', null, InputArgument::OPTIONAL, 'Neo4j source server port.');
        $command->addOption('source-user', null, InputArgument::OPTIONAL, 'Neo4j source server username.');
        $command->addOption('source-password', null, InputArgument::OPTIONAL, 'Neo4j source server password.');
        $command->addOption('read-batch-size', null, InputArgument::OPTIONAL, 'The number of nodes and relations to read at once.');
        $command->addOption('node-batch-size', null, InputArgument::OPTIONAL, 'The number of nodes to write as part of a single cypher query or batch.');
        $command->addOption('relation-batch-size', null, InputArgument::OPTIONAL, 'The number of relations to write as part of a single cypher query or batch.');
        $command->addOption('ignore-relation-properties', null, InputArgument::OPTIONAL, 'Comma separated values of properties to be ignored for relations. (eg. creationDate,modificationDate)');
        $command->addOption('preserve-ids', null, InputArgument::OPTIONAL, 'Comma separated list of label attributes to treat as Node IDs. (eg. Job.ownerId,Action.userId)');
    }
    
    public static function configureOutputOptions(BaseCommand $command)
    {
        $command->addOption('output', null, InputArgument::OPTIONAL, 'Cypher output filename. If unspecified, will use STDOUT. Set to \'default\' to use dump-[source-host]-[yyyyMMdd]-[hhmmss].cypher');
        $command->addOption('clean', null, InputArgument::OPTIONAL, 'Set to false not to clean target database before importing. By default all nodes and relations will be removed.');
        $command->addOption('import-label', null, InputArgument::OPTIONAL, 'The name of the label set on imported nodes to create a temporary index with which to quicker identify nodes when transferring relations. This label and the index on it will be removed after the import.');
        $command->addOption('import-id-key', null, InputArgument::OPTIONAL, 'The name of the key used to hold the node IDs as imported. This attribute and the index on it will be removed after the import.');
        $command->addOption('transactional', null, InputArgument::OPTIONAL, 'Set to true to wrap all cyphers in a transaction. Non-transactional by default.');
    }

    public static function makeSourceConnection($args)
    {
        return new Neo4jConnection(
            Neo4jTransfer::getWithDefault($args, 'source-host', 'localhost'),
            Neo4jTransfer::getWithDefault($args, 'source-port', 7474),
            Neo4jTransfer::getWithDefault($args, 'source-user', 'neo4j'),
            Neo4jTransfer::getWithDefault($args, 'source-password', 'neo4j')
        );
    }

    public static function makeDumpFileName($sourceHost, OutputInterface $output=null, $timestamp=null)
    {
        if (!isset($timestamp)) {
            $timestamp = date('Ymd-His');
        }
        $file = sprintf('dump-%s-%s.cypher', $sourceHost, $timestamp);
        if (isset($output)) {
            $output->writeln('Using default output file: ');
            $output->writeln($file."\n");
        }
        return $file;
    }
    
    protected static function parseLabelAttributes($str)
    {
        $labelAttributes = array();
        $strParts = explode(',', $str);
        foreach ($strParts as $labelAttrStr) {
            $labelAttrParts = explode('.', $labelAttrStr);
            if (count($labelAttrParts) != 2) {
                throw new InvalidArgumentException(
                    sprintf(
                        'Invalid label attribute "%s" in "%s".',
                        $labelAttrStr,
                        $str
                    )  
                );
            }
            if (!isset($labelAttributes[$labelAttrParts[0]])) {
                $labelAttributes[$labelAttrParts[0]] = array();
            }
            $labelAttributes[$labelAttrParts[0]][$labelAttrParts[1]] = $labelAttrParts[1];
        }
        return $labelAttributes;
    }
    
    public static function makeReadArguments(InputInterface $input, $readBatchSize=300, $nodeBatchSize=150, $relationBatchSize=25)
    {
        $args = $input->getOptions();
        $source = static::makeSourceConnection($args);
        $importLabel = Neo4jTransfer::getWithDefault($args, 'import-label', '_ilb');
        $importIdKey = Neo4jTransfer::getWithDefault($args, 'import-id-key', '_iid');
        $readBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'read-batch-size', $readBatchSize));
        $nodeBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'node-batch-size', $nodeBatchSize));
        $relationBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'relation-batch-size', $relationBatchSize));
        $clean = Neo4jTransfer::getWithDefault($args, 'clean', 'true') === 'true';
        $transactional = Neo4jTransfer::getWithDefault($args, 'transactional', 'false') === 'true';
        $ignoredRelationProperties = Neo4jTransfer::getWithDefault($args, 'ignore-relation-properties', '');
        $ignoredRelationProperties = explode(',', $ignoredRelationProperties);
        $preserveIdsStr = Neo4jTransfer::getWithDefault($args, 'preserve-ids', '');
        $preserveIds = static::parseLabelAttributes($preserveIdsStr);
        $file = Neo4jTransfer::getWithDefault($args, 'output', null);
        return array($source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, $file, 
            $clean, $transactional, $ignoredRelationProperties, $preserveIds);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        static::executeDump($input, $output);
    }
    
    public static function executeDump(InputInterface $input, OutputInterface $output)
    {
        /** @var Neo4jConnection $source */
        list(
            $source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, $file, $clean, 
            $transactional, $ignoredRelationProperties, $preserveIds
            ) = static::makeReadArguments($input);
        if (isset($file) && ($file == 'default')) {
            $file = static::makeDumpFileName($source->getHost(), $output);
        }
        if (isset($file)) {
            $file = fopen($file, 'w+');
        }
        Neo4jTransfer::dump($source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, 
            $clean, $transactional, $ignoredRelationProperties, $preserveIds, $file, $output);
        if (isset($file)) {
            fclose($file);
        }
    }
}