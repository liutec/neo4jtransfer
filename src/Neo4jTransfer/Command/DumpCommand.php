<?php

namespace Neo4jTransfer\Command;

use Neo4jTransfer\Neo4jConnection;
use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Command\Command as BaseCommand;
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
        $command->addOption('source-host', null, InputArgument::OPTIONAL, 'Neo4j Source Hostname.');
        $command->addOption('source-port', null, InputArgument::OPTIONAL, 'Neo4j Source Port.');
        $command->addOption('source-user', null, InputArgument::OPTIONAL, 'Neo4j Source Username.');
        $command->addOption('source-password', null, InputArgument::OPTIONAL, 'Neo4j Password.');
        $command->addOption('read-batch-size', null, InputArgument::OPTIONAL, 'Read batch size for nodes and relations.');
        $command->addOption('node-batch-size', null, InputArgument::OPTIONAL, 'Write batch size for nodes.');
        $command->addOption('relation-batch-size', null, InputArgument::OPTIONAL, 'Write batch size for relations.');
    }
    
    public static function configureOutputOptions(BaseCommand $command)
    {
        $command->addOption('output', null, InputArgument::OPTIONAL, 'Output filename (set to \'default\' to use dump-[source-host]-[timestamp].cypher)');
        $command->addOption('clean', null, InputArgument::OPTIONAL, 'Clean target database before importing.');
        $command->addOption('import-label', null, InputArgument::OPTIONAL, 'The name of the label set on imported nodes.');
        $command->addOption('import-id-key', null, InputArgument::OPTIONAL, 'The name of the key used to hold the node IDs as imported.');
        $command->addOption('transactional', null, InputArgument::OPTIONAL, 'If true, wrap import in a transaction.');
        $command->addOption('ignore-relation-properties', null, InputArgument::OPTIONAL, 'Comma separated values of properties to be ignored on relations.');
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
    
    public static function makeReadArguments(InputInterface $input)
    {
        $args = $input->getOptions();
        $source = static::makeSourceConnection($args);
        $importLabel = Neo4jTransfer::getWithDefault($args, 'import-label', '_ilb');
        $importIdKey = Neo4jTransfer::getWithDefault($args, 'import-id-key', '_iid');
        $readBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'read-batch-size', 300));
        $nodeBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'node-batch-size', 150));
        $relationBatchSize = intval(Neo4jTransfer::getWithDefault($args, 'relation-batch-size', 25));
        $clean = Neo4jTransfer::getWithDefault($args, 'clean', true);
        $transactional = Neo4jTransfer::getWithDefault($args, 'transactional', false);
        $ignoredRelationProperties = Neo4jTransfer::getWithDefault($args, 'ignore-relation-properties', '');
        $ignoredRelationProperties = explode(',', $ignoredRelationProperties);
        $file = Neo4jTransfer::getWithDefault($args, 'output', null);
        return array($source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, $file, 
            $clean, $transactional, $ignoredRelationProperties);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        static::executeDump($input, $output);
    }
    
    public static function executeDump(InputInterface $input, OutputInterface $output)
    {
        list(
            $source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, $file, $clean, 
            $transactional, $ignoredRelationProperties
            ) = static::makeReadArguments($input);
        if (isset($file) && ($file == 'default')) {
            $file = static::makeDumpFileName($source->getHost(), $output);
        }
        if (isset($file)) {
            $file = fopen($file, 'w+');
        }
        Neo4jTransfer::dump($source, $importLabel, $importIdKey, $readBatchSize, $nodeBatchSize, $relationBatchSize, 
            $clean, $transactional, $ignoredRelationProperties, $file, $output);
        if (isset($file)) {
            fclose($file);
        }
    }
}