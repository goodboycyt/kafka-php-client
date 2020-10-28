## kafka-php-client

### composer install
`composer require linwanfeng/kafka-php-sdk:2.0`

### composer update
`composer update linwanfeng/kafka-php-sdk`

### Produce by Rdkafka
```php
<?php
use kafkaPhp\KafkaProducer;
use kafkaPhp\KafkaException;
try{
    $kafkaClient = new KafkaProducer('127.0.0.1:9092');
    $kafkaClient->sendMsg('topic', 'msg');
}catch (KafkaException $e){
    echo $e->getErrorMessage();die;
}
```
### Produce by socket
```php
<?php
use kafkaPhp\KafkaSkProducer;
use kafkaPhp\KafkaException;
try{
   $kafkaClient = new KafkaSkProducer('***.***.*.***', '***');
   $response = $kafkaClient->sendMsg('topic', 'msg');
}catch (KafkaException $e){
    echo $e->getErrorMessage();
}
```
### Consumer
```php
<?php
require '../vendor/autoload.php';

use kafkaPhp\KafkaClient;
use kafkaPhp\KafkaException;

try{
    $kafkaClient = new KafkaClient('127.0.0.1:9092', 1000);
    $kafkaClient->initConsumer(['topic1','topic2'], 1);
    while(true){
        $r = $kafkaClient->getMsg(2, 1000);
        ...
    }

}catch (KafkaException $e){
    echo $e->getErrorMessage();die;
}
```