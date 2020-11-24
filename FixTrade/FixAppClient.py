#!/usr/bin/env python3

import sys
from datetime import datetime
import threading

from FixTrade.FixSocketHandler import FixSocketHandler
from FixTrade.FixParser import FixParser


class FixAppClient:

    def __init__(self, host, port, sender_comp_id, target_comp_id, send_seq_num):
        self.fix_client_sock = FixSocketHandler()
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.send_seq_num = send_seq_num
        self.current_seq_num = int(send_seq_num)

    def create_login_request(self):

        login_list = []

        login_list.append(("35", "A"))  # MsgType
        login_list.append(("34", self.send_seq_num))  # MsgSeqNum
        login_list.append(("49", self.sender_comp_id))  # SenderCompID
        login_list.append(("52", self.getSendingTime()))  # SendingTime
        login_list.append(("56", self.target_comp_id))  # TargetCompID
        login_list.append(("98", "0"))  # EncryptMethod
        login_list.append(("108", "30"))  # HeartBeatInt
        login_list.append(("141", "N"))  # ResetSeqNumFlag

        login_request = b''
        for login_tag in login_list:
            login_request = login_request + bytes(login_tag[0] + "=" + login_tag[1], encoding="utf-8") + b'\x01'

        bodyLength = len(login_request)  # 9 - BodyLength

        login_request = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9="+str(bodyLength), encoding="utf-8") + b'\x01' + login_request

        checkSumStr = self.getCheckSum(login_request)

        login_request = login_request + bytes("10="+checkSumStr, encoding="utf-8") + b'\x01'

        self.current_seq_num += 1

        return login_request

    def create_heartbeat_message(self):
        message_list = []

        message_list.append(("35", "0"))  # MsgType
        message_list.append(("34", str(self.current_seq_num)))  # MsgSeqNum
        message_list.append(("49", self.sender_comp_id))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", self.target_comp_id))  # TargetCompID

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        body_length = len(message) # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9="+str(body_length), encoding="utf-8") + b'\x01' + message

        check_sum_str = self.getCheckSum(message)

        message = message + bytes("10="+check_sum_str, encoding="utf-8") + b'\x01'

        self.current_seq_num += 1

        return message

    def create_new_order(self, symbol, side, quantity, price):

        message_list = []

        message_list.append(("35", "D"))  # MsgType
        message_list.append(("34", str(self.current_seq_num)))  # MsgSeqNum
        message_list.append(("49", self.sender_comp_id))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", self.target_comp_id))  # TargetCompID
        message_list.append(("55", symbol))  # Symbol
        message_list.append(("54", side))  # Side
        message_list.append(("38", quantity))  # Quantity
        message_list.append(("44", price))  # Price
        message_list.append(("40", "2"))  # OrdType
        message_list.append(("59", "0"))  # TimeInForce
        message_list.append(("1", "H121"))  # Account
        message_list.append(("47", "A"))  # AP FLAG
        message_list.append(("114", "N"))  # LocateReqd
        message_list.append(("11", str(self.current_seq_num)))  # ClOrdID
        message_list.append(("21", "3"))  # HandlInst
        message_list.append(("60", self.getSendingTime()))  # TransactTime

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        bodyLength = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9="+str(bodyLength), encoding="utf-8") + b'\x01' + message

        checkSumStr = self.getCheckSum(message)

        message = message + bytes("10="+checkSumStr, encoding="utf-8") + b'\x01'

        self.current_seq_num += 1

        return message

    def create_replace_order(self, orig_cl_ord_id, symbol, side, quantity, price):

        message_list = []

        message_list.append(("35", "G"))  # MsgType
        message_list.append(("34", str(self.current_seq_num)))  # MsgSeqNum
        message_list.append(("49", self.sender_comp_id))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", self.target_comp_id))  # TargetCompID
        message_list.append(("55", symbol))  # Symbol
        message_list.append(("54", side))  # Side
        message_list.append(("38", quantity))  # Quantity
        message_list.append(("44", price))  # Price
        message_list.append(("40", "2"))  # OrdType
        message_list.append(("59", "0"))  # TimeInForce
        message_list.append(("1", "H121"))  # Account
        message_list.append(("47", "A"))  # AP FLAG
        message_list.append(("114", "N"))  # LocateReqd
        message_list.append(("11", str(self.current_seq_num)))  # ClOrdID
        message_list.append(("21", "3"))  # HandlInst
        message_list.append(("60", self.getSendingTime()))  # TransactTime
        message_list.append(("41", orig_cl_ord_id))  # OrigClOrdID

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        bodyLength = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9="+str(bodyLength), encoding="utf-8") + b'\x01' + message

        checkSumStr = self.getCheckSum(message)

        message = message + bytes("10="+checkSumStr, encoding="utf-8") + b'\x01'

        self.current_seq_num += 1

        return message

    def create_cancel_order(self, orig_cl_ord_id, symbol, side, quantity):

        message_list = []

        message_list.append(("35", "F"))  # MsgType
        message_list.append(("34", str(self.current_seq_num)))  # MsgSeqNum
        message_list.append(("49", self.sender_comp_id))  # SenderCompID
        message_list.append(("52", self.getSendingTime()))  # SendingTime
        message_list.append(("56", self.target_comp_id))  # TargetCompID
        message_list.append(("55", symbol))  # Symbol
        message_list.append(("54", side))  # Side
        message_list.append(("38", quantity))  # Quantity
        message_list.append(("11", str(self.current_seq_num)))  # ClOrdID
        message_list.append(("60", self.getSendingTime()))  # TransactTime
        message_list.append(("41", orig_cl_ord_id))  # OrigClOrdID

        message = b''
        for message_tag in message_list:
            message = message + bytes(message_tag[0] + "=" + message_tag[1], encoding="utf-8") + b'\x01'

        bodyLength = len(message)  # 9 - BodyLength

        message = bytes("8=FIX.4.2", encoding="utf-8") + b'\x01' + bytes("9="+str(bodyLength), encoding="utf-8") + b'\x01' + message

        checkSumStr = self.getCheckSum(message)

        message = message + bytes("10="+checkSumStr, encoding="utf-8") + b'\x01'

        self.current_seq_num += 1

        return message


    def getSendingTime(self):
        return datetime.utcnow().strftime('%Y%m%d-%H:%M:%S.%f')[:-3]

    def getCheckSum(self, fixMessage):
        checkSum = 0
        for byte in fixMessage:
            checkSum = checkSum + int(byte)
        checkSumStr = str(checkSum % 256)
        return checkSumStr.zfill(3)

    def start_sending_heartbeats(self):

        heartbeat_thread = threading.Timer(30.0, self.start_sending_heartbeats, [])
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        heartbeat = self.create_heartbeat_message()
        self.fix_client_sock.send(heartbeat)

    def start(self):

        # Open Connection to FIX Server
        self.fix_client_sock.connect(self.host, self.port)

        # Send login request
        request = self.create_login_request()
        print("Sending Login Request:" + str(FixParser.parse_fix_bytes(request)))
        self.fix_client_sock.send(request)

        # Start sending Heartbeats
        self.start_sending_heartbeats()

        try:
            while True:
                input_text = input("new / replace / cancel / get : ")
                input_list = input_text.split(" ")
                if input_list:
                    if input_list[0] == "get":
                        received_messages = self.fix_client_sock.receive()
                        if not received_messages:
                            print("No received messages")
                        else:
                            for message in received_messages:
                                fix_dict = FixParser.parse_fix_bytes(message)
                                if fix_dict["35"] == "A":
                                    print("Login Result: " + str(fix_dict))
                                elif fix_dict["35"] == "0":
                                    print("Heartbeat: " + str(fix_dict))
                                elif fix_dict["35"] == "8":
                                    cl_ord_id = fix_dict["11"]
                                    if fix_dict["39"] == "0":
                                        print("New Order Ack - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    elif fix_dict["39"] == "E":
                                        print("Pending Replace - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    elif fix_dict["39"] == "5":
                                        print("Replaced - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    elif fix_dict["39"] == "6":
                                        print("Pending Cancel - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    elif fix_dict["39"] == "4":
                                        print("Canceled - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    elif fix_dict["39"] == "1":
                                        symbol = fix_dict["55"]
                                        side = fix_dict["54"]
                                        last_shares = fix_dict["32"]
                                        last_price = fix_dict["31"]
                                        print(f"Partially Filled - ClOrdID: {cl_ord_id} Symbol: {symbol} Side: {side} " +
                                              f"LastShares: {last_shares} LastPx: {last_price} " + str(fix_dict))
                                    elif fix_dict["39"] == "2":
                                        symbol = fix_dict["55"]
                                        side = fix_dict["54"]
                                        last_shares = fix_dict["32"]
                                        last_price = fix_dict["31"]
                                        cum_qty = fix_dict["14"]
                                        avg_px = fix_dict["6"]
                                        print(f"Filled - ClOrdID: {cl_ord_id} Symbol: {symbol} Side: {side} " +
                                              f"LastShares: {last_shares} LastPx: {last_price} " +
                                              f"CumQty: {cum_qty} AvgPx: {avg_px} " + str(fix_dict))
                                    elif fix_dict["39"] == "8":
                                        print("Rejected - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                    else:
                                        print(str(fix_dict))
                                elif fix_dict["35"] == "9":
                                    cl_ord_id = fix_dict["11"]
                                    print("Order Cancel/Replace Rejected - ClOrdID: " + cl_ord_id + "  " + str(fix_dict))
                                else:
                                    print(str(fix_dict))
                    elif input_list[0] == "new":
                        if len(input_list) == 5:
                            symbol = input_list[1]
                            side = input_list[2]
                            quantity = input_list[3]
                            price = input_list[4]
                            new_order = self.create_new_order(symbol, side, quantity, price)
                            self.fix_client_sock.send(new_order)
                        else:
                            print("Usage: new <symbol> <side> <quantity> <price>")
                    elif input_list[0] == "replace":
                        if len(input_list) == 6:
                            orig_cl_ord_id = input_list[1]
                            symbol = input_list[2]
                            side = input_list[3]
                            quantity = input_list[4]
                            price = input_list[5]
                            amend_order = self.create_replace_order(orig_cl_ord_id, symbol, side, quantity, price)
                            self.fix_client_sock.send(amend_order)
                        else:
                            print("Usage: replace <OrigClOrdID> <symbol> <side> <quantity> <price>")
                    elif input_list[0] == "cancel":
                        if len(input_list) == 5:
                            orig_cl_ord_id = input_list[1]
                            symbol = input_list[2]
                            side = input_list[3]
                            quantity = input_list[4]
                            cancel_order = self.create_cancel_order(orig_cl_ord_id, symbol, side, quantity)
                            self.fix_client_sock.send(cancel_order)
                        else:
                            print("Usage: cancel <OrigClOrdID> <symbol> <side> <quantity>")


        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.fix_client_sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("usage:", sys.argv[0], "<host> <port> <sender_comp_id> <target_comp_id> <send_seq_num>")
        sys.exit(1)

    main_host, main_port = sys.argv[1], int(sys.argv[2])
    main_sender_comp_id = sys.argv[3]
    main_target_comp_id = sys.argv[4]
    main_send_seq_num = sys.argv[5]

    fix_app_client = FixAppClient(main_host, main_port, main_sender_comp_id, main_target_comp_id, main_send_seq_num)

    # Start the FIX Client
    fix_app_client.start()
