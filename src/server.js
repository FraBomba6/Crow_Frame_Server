const express = require('express')
const cluster = require('cluster')
const fs = require("fs")
const http = require('http')
const https = require('https')
const numCPUs = require('os').cpus().length
const amqp = require('amqplib')
const db = require('./db')

const port = 3000

let experiments = fs.readFileSync('../experiments.txt').toString().split("\n")

if (cluster.isMaster) {
    let reqNum = 0;
    console.log(`Master ${process.pid} is running`)

    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('exit', () => {
        cluster.fork();
    });

    cluster.on('message', () => {
        reqNum++;
    });

    const app = express()
    app.use(express.json())
    app.listen(port, () => {
        console.log("Ready for statistics...")
    })
    app.get('get', (req, res) => {
        res.status(200)
        res.json({reqNum: reqNum})
        res.socket.end()
    })

} else {
    let inConnection = amqp.connect()
    let outConnection = amqp.connect()
    let inChannel = inConnection.then(connection => connection.createChannel())
    let outChannel = outConnection.then(connection => connection.createChannel())

    for (let i = 0; i < experiments.length - 1; i++) {
        if (experiments[i] !== "") {
            work(experiments[i])
        }
    }

    const app = express()
    app.use(express.json())
    app.listen(port, () => {
        console.log("Listening on port " + port + " as Worker " + cluster.worker.id + " running @ process " + cluster.worker.process.pid + "!");
    })

    app.post('/log', (req, res) => {
        process.send({})
        res.status(200).end()
        res.socket.end()
        req.body.server_time = new Date().getTime()
        if (req.body.sequence === 0) {
            let geolocation = lookup(req.body.details.ip)
            geolocation.then(details => {
                req.body.details = Object.assign({}, req.body.details, details)
                manageRequest(req)
            })
        } else {
            if (req.body.type === 'queryResults') {
                let index = 0;
                for (const url of req.body.details['urlArray']) {
                    let path = "../query_page/" + req.body.task + "/" + req.body.batch + "/" + req.body.worker + "/page_" + index.toString() + ".html";
                    let content = "";
                    let protocol = url.split(':')[0]
                    let client = getClient(protocol)
                    let req = client.request(url, (res) => {
                        res.setEncoding("utf8");
                        res.on("data", function (chunk) {
                            content += chunk;
                        });
                        res.on("end", function () {
                            fs.writeFile(path, content, err => console.log(err))
                        });
                    });
                    req.end();
                    index++;
                }
            }
            manageRequest(req)
        }
    })

    function manageRequest(req) {
        let log = req.body
        let queue = `${log.task}_${log.batch}`
        let msg = JSON.stringify(log)
        let done

        inChannel.then((channel) => {
            channel.assertQueue(
                queue,
                {durable: true}
            )
            do {
                done = channel.sendToQueue(queue, Buffer.from(msg), {persistent: true})
            } while (!done)
        })
    }

    function work(queue) {
        console.log('Worker ' + cluster.worker.id + ' working on ' + queue + ' queue')
        outChannel.then(channel => {
            channel.assertQueue(
                queue,
                {durable: true}
            )

            channel.consume(queue, async function (msg) {
                let parsed_msg = JSON.parse(msg.content.toString())
                parsed_msg['sequence'] += Math.floor(Math.random() * 1001) // FOR TESTING ONLY
                db.log([parsed_msg['worker'], parsed_msg['sequence'], parsed_msg['batch'], parsed_msg['client_time'], parsed_msg['details'], parsed_msg['task'], parsed_msg['type'], parsed_msg['server_time']], parsed_msg['task'], parsed_msg['batch'])
                    .then(() => {
                        channel.ack(msg)
                    })
                    .catch((e) => {
                        channel.nack(msg)
                    })
            })
        })
    }

    function lookup(ip) {
        let url = 'https://ip-api.com/json/' + ip + '?fields=continentCode,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,reverse,mobile,proxy,hosting'
        return new Promise(resolve =>
            https.get(url, res => {
                let data = ''

                res.on('data', (chunk) => {
                    data += chunk
                });
                res.on('end', () => {
                    resolve(JSON.parse(data))
                });
            }))
    }

    function getClient(protocol) {
        if (protocol === 'http') return http
        else return https
    }
}