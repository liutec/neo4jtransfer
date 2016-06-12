<?php

namespace Neo4jTransfer\Console;

use Neo4jTransfer\Command\DumpCommand;
use Neo4jTransfer\Command\ImportCommand;
use Neo4jTransfer\Command\TransferCommand;
use Neo4jTransfer\Command\DirectTransferCommand;
use Symfony\Component\Console\Application as ConsoleApplication;
use Neo4jTransfer\Neo4jTransfer;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class Application extends ConsoleApplication
{
    function __construct()
    {
        parent::__construct('Neo4jTransfer', Neo4jTransfer::VERSION);
    }

    /**
     * Initializes all commands
     */
    protected function getDefaultCommands()
    {
        $commands = parent::getDefaultCommands();
        $commands[] = new DumpCommand();
        $commands[] = new ImportCommand();
        $commands[] = new TransferCommand();
        $commands[] = new DirectTransferCommand();
        return $commands;
    }

    /**
     * {@inheritDoc}
     */
    public function run(InputInterface $input = null, OutputInterface $output = null)
    {
        return parent::run($input, $output);
    }

    /**
     * {@inheritDoc}
     */
    public function doRun(InputInterface $input, OutputInterface $output)
    {
        return parent::doRun($input, $output);
    }
}