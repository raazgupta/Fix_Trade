#!/usr/bin/env python3

import sys
from datetime import datetime
import select
import threading

from FixTrade.FixSocketHandler import FixSocketHandler
from FixTrade.FixParser import FixParser

class FixAppServer:

    def __init__(self, host, port):
        self.fix_server_sock = FixSocketHandler()
        self.host = host
        self.port = port
        self.socket_list = []
        self.clients_dict = {}

    def create_login_response(self, client_dict):

        login_list = []

        login_list.append(("35", "A"))  # MsgType
        login_list.append(("34", str(client_dict["current_seq_num"])))  # MsgSeqNum
        login_list.append(("49", client_dict["sender_comp_id"]))  # SenderCompID
        login_list.append(("52", self.getSendingTime()))  # SendingTime
        login_list.append(("56", client_dict["target_comp_id"]))  # TargetCompID
        login_list.append(("98", "0"))  # EncryptMethod
        login_list.append(("108", "30"))  # HeartBeatInt

        login_response = b''
        for login_tag in login_list:
            login_response = login_response + bytes(login_tag[0] + "=" + login_tag[1], encoding="utf-8") + b'\x01'

        bodyLength = len(login_response)  # 9 - BodyLength

        login_response = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9=" + str(bodyLength),
                                                                                encoding="utf-8") + b'\x01' + login_response

        checkSumStr = self.getCheckSum(login_response)

        login_response = login_response + bytes("10=" + checkSumStr, encoding="utf-8") + b'\x01'

        client_dict["current_seq_num"] += 1

        return login_response

    def create_heartbeat_message(self, client_dict):
        message_list = []

        message_list.append(("35", "0"))  # MsgType
        message_list.append(("34", str(client_dict["current_seq_num"])))  # MsgSeqNum
        message_list.append(("49", client_dict["sender_comp_id"]))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", client_dict["target_comp_id"]))  # TargetCompID

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        body_length = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9=" + str(body_length),
                                                                         encoding="utf-8") + b'\x01' + message

        check_sum_str = self.getCheckSum(message)

        message = message + bytes("10=" + check_sum_str, encoding="utf-8") + b'\x01'

        client_dict["current_seq_num"] += 1

        return message

    def create_new_order_ack(self, client_dict, new_order_dict):
        message_list = []

        message_list.append(("35", "8"))  # MsgType
        message_list.append(("34", str(client_dict["current_seq_num"])))  # MsgSeqNum
        message_list.append(("49", client_dict["sender_comp_id"]))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", client_dict["target_comp_id"]))  # TargetCompID
        message_list.append(("11", new_order_dict["11"]))  # ClOrdID
        message_list.append(("17", str(client_dict["current_seq_num"])))  # ExecID
        message_list.append(("20", "0"))  # ExecTransType
        message_list.append(("150", "0"))  # ExecType
        message_list.append(("151", new_order_dict["38"]))  # LeavesQty
        message_list.append(("37", str(client_dict["current_seq_num"])))  # OrderID
        message_list.append(("38", new_order_dict["38"]))  # OrderQty
        message_list.append(("39", "0"))  # OrdStatus
        message_list.append(("40", new_order_dict["40"]))  # OrdType
        message_list.append(("47", new_order_dict["47"]))  # Rule80A
        message_list.append(("54", new_order_dict["54"]))  # Side
        message_list.append(("55", new_order_dict["55"]))  # Symbol
        message_list.append(("59", new_order_dict["59"]))  # TimeInForce
        message_list.append(("60", self.getSendingTime()))  # TransactTime
        message_list.append(("9996", "Exchange_Order_ID"))  # Custom
        message_list.append(("44", new_order_dict["44"]))  # Price
        message_list.append(("1", new_order_dict["1"]))  # Account
        message_list.append(("31", "0"))  # LastPx
        message_list.append(("32", "0"))  # LastShares

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        body_length = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9=" + str(body_length),
                                                                         encoding="utf-8") + b'\x01' + message

        check_sum_str = self.getCheckSum(message)

        message = message + bytes("10=" + check_sum_str, encoding="utf-8") + b'\x01'

        client_dict["current_seq_num"] += 1

        return message

    def create_replace_ack(self, client_dict, replace_request_dict, status):
        message_list = []

        message_list.append(("35", "8"))  # MsgType --
        message_list.append(("34", str(client_dict["current_seq_num"])))  # MsgSeqNum --
        message_list.append(("49", client_dict["sender_comp_id"]))  # SenderCompID --
        message_list.append(("52", self.getSendingTime()))  # SendingTime --
        message_list.append(("56", client_dict["target_comp_id"]))  # TargetCompID --
        message_list.append(("11", replace_request_dict["11"]))  # ClOrdID --
        message_list.append(("17", str(client_dict["current_seq_num"])))  # ExecID --
        message_list.append(("20", "0"))  # ExecTransType --
        message_list.append(("150", status))  # ExecType --
        message_list.append(("151", replace_request_dict["38"]))  # LeavesQty --
        message_list.append(("37", str(client_dict["current_seq_num"])))  # OrderID --
        message_list.append(("38", replace_request_dict["38"]))  # OrderQty --
        message_list.append(("39", status))  # OrdStatus --
        message_list.append(("40", replace_request_dict["40"]))  # OrdType --
        message_list.append(("47", replace_request_dict["47"]))  # Rule80A --
        message_list.append(("54", replace_request_dict["54"]))  # Side --
        message_list.append(("55", replace_request_dict["55"]))  # Symbol --
        message_list.append(("59", replace_request_dict["59"]))  # TimeInForce --
        message_list.append(("60", self.getSendingTime()))  # TransactTime --
        message_list.append(("9996", "Exchange_Order_ID"))  # Custom --
        message_list.append(("44", replace_request_dict["44"]))  # Price --
        message_list.append(("1", replace_request_dict["1"]))  # Account --
        message_list.append(("31", "0"))  # LastPx --
        message_list.append(("32", "0"))  # LastShares --
        message_list.append(("14", "0"))  # CumQty --
        message_list.append(("41", replace_request_dict["11"]))  # OrigClOrdID --


        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        body_length = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9=" + str(body_length),
                                                                         encoding="utf-8") + b'\x01' + message

        check_sum_str = self.getCheckSum(message)

        message = message + bytes("10=" + check_sum_str, encoding="utf-8") + b'\x01'

        client_dict["current_seq_num"] += 1

        return message

    def create_cancel_ack(self, client_dict, cancel_request_dict, status):
        message_list = []

        message_list.append(("35", "8"))  # MsgType --
        message_list.append(("34", str(client_dict["current_seq_num"])))  # MsgSeqNum --
        message_list.append(("49", client_dict["sender_comp_id"]))  # SenderCompID --
        message_list.append(("52", self.getSendingTime()))  # SendingTime --
        message_list.append(("56", client_dict["target_comp_id"]))  # TargetCompID --
        message_list.append(("11", cancel_request_dict["11"]))  # ClOrdID --
        message_list.append(("17", str(client_dict["current_seq_num"])))  # ExecID --
        message_list.append(("20", "0"))  # ExecTransType --
        message_list.append(("150", status))  # ExecType --
        message_list.append(("151", "0"))  # LeavesQty --
        message_list.append(("37", str(client_dict["current_seq_num"])))  # OrderID --
        message_list.append(("38", cancel_request_dict["38"]))  # OrderQty --
        message_list.append(("39", status))  # OrdStatus --
        message_list.append(("40", "2"))  # OrdType --
        message_list.append(("47", "A"))  # Rule80A --
        message_list.append(("54", cancel_request_dict["54"]))  # Side --
        message_list.append(("55", cancel_request_dict["55"]))  # Symbol --
        message_list.append(("59", "0"))  # TimeInForce --
        message_list.append(("60", self.getSendingTime()))  # TransactTime --
        message_list.append(("9996", "Exchange_Order_ID"))  # Custom --
        message_list.append(("44", "0"))  # Price --
        message_list.append(("1", "xyz"))  # Account --
        message_list.append(("31", "0"))  # LastPx --
        message_list.append(("32", "0"))  # LastShares --
        message_list.append(("14", "0"))  # CumQty --
        message_list.append(("41", cancel_request_dict["11"]))  # OrigClOrdID --
        message_list.append(("6", "0")) # AvgPx --
        message_list.append(("15", "JPY")) # Currency --
        message_list.append(("120", "JPY")) # SettlCurrency --
        message_list.append(("100", "XTKS")) # ExDestination --


        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        body_length = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9=" + str(body_length),
                                                                         encoding="utf-8") + b'\x01' + message

        check_sum_str = self.getCheckSum(message)

        message = message + bytes("10=" + check_sum_str, encoding="utf-8") + b'\x01'

        client_dict["current_seq_num"] += 1

        return message

    def getSendingTime(self):
        return datetime.utcnow().strftime('%Y%m%d-%H:%M:%S.%f')[:-3]

    def getCheckSum(self, fixMessage):
        checkSum = 0
        for byte in fixMessage:
            checkSum = checkSum + int(byte)
        checkSumStr = str(checkSum % 256)
        return checkSumStr.zfill(3)

    def start_sending_heartbeats(self, fix_client_sock, client_dict):

        heartbeat_thread = threading.Timer(30.0, self.start_sending_heartbeats, [fix_client_sock, client_dict])
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        heartbeat = self.create_heartbeat_message(client_dict)
        fix_client_sock.send(heartbeat)
        print(f"Sending Heartbeat to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(heartbeat)} ")

    def start(self):

        # Listen for connections from FIX Client
        self.fix_server_sock.listen(self.host, self.port)
        self.socket_list.append(self.fix_server_sock.sock)

        try:
            # Check for incoming messages
            while True:

                read_sockets, _, exception_sockets = select.select(self.socket_list, [], self.socket_list, 0)

                for notified_socket in read_sockets:
                    # If notified socket is a server socket - new connection, accept it
                    if notified_socket == self.fix_server_sock.sock:
                        # Accept new connection
                        client_socket, client_address = self.fix_server_sock.sock.accept()
                        print(f"Accepting new connection from {client_address}")
                        self.socket_list.append(client_socket)
                    else:
                        # Receive messages from client
                        fix_client_sock = FixSocketHandler(notified_socket)
                        received_messages = fix_client_sock.receive()

                        for received_message in received_messages:

                            fix_dict = FixParser.parse_fix_bytes(received_message)

                            if fix_dict["35"] == "A":
                                # Found a login request, send a login response
                                print("Received Login Request")
                                sender_comp_id = fix_dict["56"]
                                target_comp_id = fix_dict["49"]

                                self.clients_dict = {notified_socket:
                                                   {"sender_comp_id": sender_comp_id,
                                                    "target_comp_id": target_comp_id,
                                                    "current_seq_num": 1}
                                               }

                                login_response = self.create_login_response(self.clients_dict[notified_socket])
                                fix_client_sock.send(login_response)
                                print("Sent Login Response")
                                # Start sending Heartbeats
                                self.start_sending_heartbeats(fix_client_sock, self.clients_dict[notified_socket])
                            elif fix_dict["35"] == "D":
                                # Found a new order, send a new order ack
                                print("Received New Order:" + str(fix_dict))
                                client_dict = self.clients_dict[notified_socket]
                                new_order_ack = self.create_new_order_ack(client_dict, fix_dict)
                                fix_client_sock.send(new_order_ack)
                                print(f"Sending New Order Ack to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(new_order_ack)} ")
                            elif fix_dict["35"] == "G":
                                # Found a replace order, send a replace ack and replace result
                                print("Received Replace Order:" + str(fix_dict))
                                client_dict = self.clients_dict[notified_socket]
                                pending_replace = self.create_replace_ack(client_dict, fix_dict, "E")
                                fix_client_sock.send(pending_replace)
                                print(f"Sending Pending Replace to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(pending_replace)} ")
                                replace_result = self.create_replace_ack(client_dict, fix_dict, "5")
                                fix_client_sock.send(replace_result)
                                print(f"Sending Replace Result to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(replace_result)} ")
                            elif fix_dict["35"] == "F":
                                # Found a cancel order, send a cancel ack and cancel result
                                print("Received Cancel Order:" + str(fix_dict))
                                client_dict = self.clients_dict[notified_socket]
                                pending_cancel = self.create_cancel_ack(client_dict, fix_dict, "6")
                                fix_client_sock.send(pending_cancel)
                                print(f"Sending Pending Cancel to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(pending_cancel)} ")
                                cancel_result = self.create_cancel_ack(client_dict, fix_dict, "4")
                                fix_client_sock.send(cancel_result)
                                print(f"Sending Cancel Result to {fix_client_sock.sock.getpeername()}: {FixParser.parse_fix_bytes(cancel_result)} ")

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            for sock in self.socket_list:
                sock.close()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("usage:", sys.argv[0], "<host> <port>")
        sys.exit(1)

    main_host, main_port = sys.argv[1], int(sys.argv[2])

    fix_app_server = FixAppServer(main_host, main_port)

    # Start the FIX Server
    fix_app_server.start()
