from flask import Flask, jsonify, request
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Dictionary to store client subscriptions {cluster_id+subscriber_id: [client_id1, client_id2, ...]}
subscriptions = {}

@app.route('/subscriptions', methods=['GET'])
def get_subscriptions():
    """Endpoint to get all subscriptions"""
    return jsonify(subscriptions)

@app.route('/subscribe', methods=['POST'])
def subscribe_client():
    """Endpoint to add a new client to a subscription"""
    try:
        data = request.get_json()
        if not all(k in data for k in ['subscription_key', 'client_id']):
            return jsonify({"error": "Missing required fields"}), 400
        
        subscription_key = data['subscription_key']  # This is the 'm' value (cluster_id + subscriber_id)
        client_id = data['client_id']
        
        # Add client to subscription
        if subscription_key in subscriptions:
            if client_id not in subscriptions[subscription_key]:
                subscriptions[subscription_key].append(client_id)
        else:
            subscriptions[subscription_key] = [client_id]
            
        logger.info(f"Updated subscriptions: {subscriptions}")
        return jsonify({
            "message": "Subscription updated successfully",
            "subscriptions": subscriptions
        }), 200
        
    except Exception as e:
        logger.error(f"Error in subscribe_client: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/unsubscribe', methods=['POST'])
def unsubscribe_client():
    """Endpoint to remove a client from a subscription"""
    try:
        data = request.get_json()
        if not all(k in data for k in ['subscription_key', 'client_id']):
            return jsonify({"error": "Missing required fields"}), 400
            
        subscription_key = data['subscription_key']
        client_id = data['client_id']
        
        if subscription_key in subscriptions and client_id in subscriptions[subscription_key]:
            subscriptions[subscription_key].remove(client_id)
            if not subscriptions[subscription_key]:  # If list is empty
                del subscriptions[subscription_key]  # Remove the key
                
        return jsonify({
            "message": "Client unsubscribed successfully",
            "subscriptions": subscriptions
        }), 200
        
    except Exception as e:
        logger.error(f"Error in unsubscribe_client: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5002, debug=False)
