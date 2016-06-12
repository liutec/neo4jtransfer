<?php
function includeIfExists($file)
{
    return file_exists($file) ? include $file : false;
}
if ((!$loader = includeIfExists(__DIR__.'/../vendor/autoload.php')) && (!$loader = includeIfExists(__DIR__.'/../../../autoload.php'))) {
    echo "Autoloader not found!\n";
    exit(1);
}
return $loader;