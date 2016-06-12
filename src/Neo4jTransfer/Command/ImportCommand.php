<?php

namespace Neo4jTransfer\Command;

use Neo4jTransfer\Neo4jConnection;
use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Command\Command as BaseCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class ImportCommand extends BaseCommand
{
    protected function configure()
    {
        static::configureImport($this);
    }
    
    public static function configureImport(BaseCommand $command, $name='import')
    {
        $command
            ->setName($name)
            ->setDescription('Import all nodes and relations into a Neo4j Server database.')
        ;

        static::configureTargetConnectionOptions($command);
        $command->addOption('input', null, InputArgument::OPTIONAL, 'Input filename');
    }
    
    public static function configureTargetConnectionOptions(BaseCommand $command)
    {
        $command->addOption('target-host', null, InputArgument::OPTIONAL, 'Neo4j Source Hostname');
        $command->addOption('target-port', null, InputArgument::OPTIONAL, 'Neo4j Source Port');
        $command->addOption('target-user', null, InputArgument::OPTIONAL, 'Neo4j Source Username');
        $command->addOption('target-password', null, InputArgument::OPTIONAL, 'Neo4j Password');
    }
    
    public static function makeTargetConnection($args)
    {
        return new Neo4jConnection(
            Neo4jTransfer::getWithDefault($args, 'target-host', 'localhost'),
            Neo4jTransfer::getWithDefault($args, 'target-port', 7474),
            Neo4jTransfer::getWithDefault($args, 'target-user', 'neo4j'),
            Neo4jTransfer::getWithDefault($args, 'target-password', 'neo4j')
        );
    }

    public static function makeWriteArguments(InputInterface $input)
    {
        $args = $input->getOptions();
        $target = static::makeTargetConnection($args);
        return array($target);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        static::executeImport($input, $output);
    }
    
    public static function executeImport(InputInterface $input, OutputInterface $output)
    {
        list($target) = static::makeWriteArguments($input);
        $file = Neo4jTransfer::getWithDefault($input->getOptions(), 'input', null);
        if (isset($file)) {
            $file = fopen($file, 'r');
        }
        Neo4jTransfer::import($target, $file, $output);
        if (isset($file)) {
            fclose($file);
        }
    }
}