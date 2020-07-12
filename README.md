## kafka-php-client

### composer install
`composer require linwanfeng/kafka-php-sdk:dev-master`

### Producer
```php
<?php
use kafkaPhp\KafkaClient;
use kafkaPhp\KafkaException;
try{
    $kafkaClient = new KafkaClient('127.0.0.1:9092', 1000);
    $kafkaClient->initProducer(0);
    $kafkaClient->sendMsg('topic', 'msg', 1000);
}catch (KafkaException $e){
    echo $e->getErrorMessage();die;
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