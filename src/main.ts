import { PrismaClient } from "@prisma/client";
import * as zmq from 'zeromq';
const prisma = new PrismaClient();

export const zmqResponder = async () => {
    const sock = new zmq.Reply();
    sock.connect('tcp://localhost:9950');

    for await (const [msg] of sock) {
        if (msg.includes("heartbeat")) {
            // Update the prisma db for the client that sent a heartbeat
            let responseMsg = "ACK heartbeat ";
            let splitMsg: string[];
            try {
                splitMsg = msg.toString('utf8').split(" ");
            } catch {
                await sock.send(msg);
                continue;
            }
            responseMsg = responseMsg.concat(splitMsg[1]);

            let client = await prisma.client.findFirst({ where: { macAddress: splitMsg[1] } });

            if (client != null) {
                await prisma.client.update({
                    where: {
                        id: client.id
                    },
                    data: {
                        lastHeartbeat: new Date()
                    }
                })
            }
            console.log(responseMsg);
            await sock.send(responseMsg);
            
        } else if (msg.includes("checkin")) {
            // Add the new checkin to the db if the checkin does not exist yet
            let responseMsg = "ACK checkin ";
            let splitMsg: string[];
            try {
                splitMsg = msg.toString('utf8').split(" ");
            } catch {
                await sock.send(msg);
                continue;
            }
            responseMsg = responseMsg.concat(splitMsg[2]);

            let client = await prisma.client.findFirst({ where: { macAddress: splitMsg[1] } });
            let event = await prisma.event.findFirst({ where: { id: client.eventId } });

            try {
                if (client != null) {
                    await prisma.checkin.create({
                        data: {
                            studentId: splitMsg[2],
                            Event: { connect: {id: event.id} }
                        }
                    })
                }
            } catch {
                console.error(`duplicate checkin:{\n\teventID: ${event.id}\n\tstudentID: ${splitMsg[2]}}`);
                await sock.send(responseMsg);
                continue;
            }
            console.log(responseMsg);
            await sock.send(responseMsg);

        } else {
            // Keep the client from crashing by just responding with what was recieved
            await sock.send(msg);
        }
    }
}

zmqResponder();