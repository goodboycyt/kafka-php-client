<?php
namespace Kafka;

use Kafka\KafkaException;

/**
 * Class KafkaClient
 *
 * Kafka Services client class, which wraps all frequently used APIs user could call to talk to kafka.
 * Users could do operations like creating a producer,producing messages,height-level consuming,low-level consuming via an KafkaClient instance.
 * For more details, please check out the Kafka API document:https://github.com/goodboycyt/kafka-php-client
 * @package Kafka
 */
class KafkaClient
{
    /**
     * Constructor
     *
     * Nothing to say.
     *
     * @param string $host The host of Kafka server with port like 193.23.1.1:9093
     * @param integer $queueMaxTime Max time(ms) for queue buffer wait.
     * @param integer $groupId If use consumer,set this value to your group id
     * @throws KafkaException
     */
    public function __construct($host, $queueMaxTime, $groupId = 1)
    {
        $host = trim($host);

        if (empty($host)) {
            throw new KafkaException("host is empty");
        }
        if (empty($queueMaxTime)) {
            throw new KafkaException("queueMaxTime is empty");
        }
        $this->host = $host;
        $this->queueMaxTime = $queueMaxTime;
        $this->groupId = $groupId;
    }

    /**
     * Producer a message
     *
     * Nothing to say.
     *
     * @param string $topic which topic to send
     * @param string $msg msg body
     * @param integer $part which partition
     * @throws KafkaException
     */
    public function prodecerMsg($topic, $msg, $part = 0)
    {
        if (empty($topic) || empty($msg)) {
            throw new KafkaException("topic or msg is empty");
        }
        $conf = new \RdKafka\Conf();
        $conf->set('api.version.request', 'true');
        $conf->set('message.send.max.retries', 2);
        $conf->set('api.version.request.timeout.ms', 5);
        $conf->set('queue.buffering.max.ms', $this->queueMaxTime);
        $conf->set('bootstrap.servers', $this->host);
        $rk = new RdKafka\Producer($conf);
        $topic = $rk->newTopic($topic);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg);
        $rk->flush(2);
    }

    /**
     * Consumer a message
     *
     * Nothing to say.
     *
     * @param array $topic which topic to get
     * @param string $num how much msg to get
     * @param integer $part which partition
     * @return array $mags
     * @throws KafkaException
     */
    public function ConsumerMsg($topic, $num, $part = 0)
    {
        if (empty($topic) || !is_numeric($num)) {
            throw new KafkaException("topic or num is empty");
        }
        $conf = new RdKafka\Conf();
        $conf->set('api.version.request', 'true');
        $conf->set('message.send.max.retries', 2);
        $conf->set('api.version.request.timeout.ms', 5);
//        $conf->set('queue.buffering.max.ms', 1);
        $conf->set('group.id', $this->groupId);
        $conf->set('bootstrap.servers', $this->host);
        $consumer = new RdKafka\KafkaConsumer($conf);

        $consumer->subscribe($topic);
        $msgs = [];
        for ($i = 0; $i < $num; $i++) {
            $message = $consumer->consume(1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $msgs[] = ['t_name'=>$message['topic_name'],'msg'=>$message['payload']];
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
//                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
//                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
        return $msgs;
    }

    private $host;
    private $queueMaxTime;
    private $groupId;
}