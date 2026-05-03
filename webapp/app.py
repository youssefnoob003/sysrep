"""
Flash Sale Ticket Booking — Flask API Gateway & UI
Zero distributed logic. Forwards gRPC requests to random backend nodes.
"""

from flask import Flask, render_template, jsonify, request
import grpc
import random
import uuid
import ticket_pb2
import ticket_pb2_grpc
import os
import argparse

app = Flask(__name__)

GRPC_PORTS = [50051, 50052, 50053]
DB_FILE = os.path.join(os.path.dirname(__file__), '..', 'database.txt')
more_users = False

def get_random_stub():
    port = random.choice(GRPC_PORTS)
    channel = grpc.insecure_channel(f'localhost:{port}')
    stub = ticket_pb2_grpc.TicketServiceStub(channel)
    return stub, port

@app.route('/')
def index():
    return render_template('index.html', more_users=more_users)

@app.route('/api/ticket-count', methods=['GET'])
def ticket_count():
    try:
        with open(DB_FILE, 'r') as f:
            count = int(f.read().strip())
        return jsonify({'count': count})
    except Exception as e:
        return jsonify({'count': -1, 'error': str(e)}), 500

@app.route('/api/buy', methods=['POST'])
def buy_ticket():
    data = request.get_json()
    buyer_name = data.get('buyer_name', 'Unknown')
    try:
        stub, port = get_random_stub()
        request_id = str(uuid.uuid4())
        response = stub.BuyTicket(
            ticket_pb2.BuyTicketRequest(buyer_name=buyer_name, request_id=request_id),
            timeout=15
        )
        return jsonify({
            'success': response.success,
            'message': response.message,
            'remaining_tickets': response.remaining_tickets,
            'served_by_port': response.served_by_port,
            'routed_to_port': port,
            'buyer_name': buyer_name
        })
    except grpc.RpcError as e:
        return jsonify({
            'success': False,
            'message': f'gRPC Error: {e.details()}',
            'buyer_name': buyer_name
        }), 500

@app.route('/api/reset', methods=['POST'])
def reset_database():
    try:
        with open(DB_FILE, 'w') as f:
            f.write('1')
        return jsonify({'success': True, 'message': 'Database reset to 1 ticket.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/reset-more', methods=['POST'])
def reset_database_more():
    try:
        with open(DB_FILE, 'w') as f:
            f.write('30')
        return jsonify({'success': True, 'message': 'Database reset to 30 tickets.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-more', action='store_true', help='Enable more users mode')
    parser.add_argument('-nodes', type=int, help='Number of nodes')
    args = parser.parse_args()
    more_users = args.more
    num_nodes = args.nodes
    if more_users:
        GRPC_PORTS = [50051 + i for i in range(num_nodes)]
    
    app.run(debug=True, port=5000)
