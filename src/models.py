"""
Data models and message schemas for the pizza shop demo system.

Defines pizza types, message schemas, and helper functions for creating
messages that conform to the architecture specifications.
"""

import uuid
import random
from datetime import datetime, timezone
from typing import Dict, List, Any, TypedDict, Optional
from enum import Enum


# ============================================================================
# Pizza Type Definitions
# ============================================================================

class PizzaComplexity(str, Enum):
    """Pizza complexity levels."""
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


class PizzaType:
    """
    Pizza type definition with cooking time and complexity.
    
    Attributes:
        name: Pizza type name
        cooking_time: Base cooking time in seconds
        complexity: Complexity level
    """
    
    def __init__(self, name: str, cooking_time: float, complexity: PizzaComplexity):
        self.name = name
        self.cooking_time = cooking_time
        self.complexity = complexity
    
    def __repr__(self) -> str:
        return f"PizzaType(name='{self.name}', cooking_time={self.cooking_time}, complexity='{self.complexity.value}')"


# Pizza menu as defined in architecture (10 types, cooking times 0.2-2.0s)
PIZZA_MENU: Dict[str, PizzaType] = {
    "Margherita": PizzaType("Margherita", 0.2, PizzaComplexity.SIMPLE),
    "Pepperoni": PizzaType("Pepperoni", 0.4, PizzaComplexity.SIMPLE),
    "Hawaiian": PizzaType("Hawaiian", 0.6, PizzaComplexity.SIMPLE),
    "Vegetarian": PizzaType("Vegetarian", 0.8, PizzaComplexity.MEDIUM),
    "BBQ Chicken": PizzaType("BBQ Chicken", 1.0, PizzaComplexity.MEDIUM),
    "Meat Lovers": PizzaType("Meat Lovers", 1.2, PizzaComplexity.MEDIUM),
    "Four Cheese": PizzaType("Four Cheese", 1.4, PizzaComplexity.COMPLEX),
    "Supreme": PizzaType("Supreme", 1.6, PizzaComplexity.COMPLEX),
    "Seafood Special": PizzaType("Seafood Special", 1.8, PizzaComplexity.COMPLEX),
    "Deluxe Everything": PizzaType("Deluxe Everything", 2.0, PizzaComplexity.COMPLEX),
}


# Pizza lists by complexity for weighted selection
SIMPLE_PIZZAS = [name for name, pizza in PIZZA_MENU.items() if pizza.complexity == PizzaComplexity.SIMPLE]
MEDIUM_PIZZAS = [name for name, pizza in PIZZA_MENU.items() if pizza.complexity == PizzaComplexity.MEDIUM]
COMPLEX_PIZZAS = [name for name, pizza in PIZZA_MENU.items() if pizza.complexity == PizzaComplexity.COMPLEX]


# ============================================================================
# Message Type Definitions (TypedDict for better type hints)
# ============================================================================

class PizzaItem(TypedDict):
    """Individual pizza item in an order."""
    pizza_type: str
    quantity: int


class OrderMessage(TypedDict):
    """Message schema for pizza-orders topic."""
    order_id: str
    timestamp: str
    source: str
    customer_name: str
    pizzas: List[PizzaItem]
    total_pizzas: int
    order_number: int


class CookedPizzaMessage(TypedDict):
    """Message schema for cooked-pizzas topic."""
    order_id: str
    timestamp: str
    cooked_by: str
    cooked_at: str
    cooking_duration_seconds: float
    customer_name: str
    pizzas: List[PizzaItem]
    total_pizzas: int
    order_number: int
    original_order: OrderMessage


class OrderEventMetadata(TypedDict, total=False):
    """Metadata for order events (optional fields)."""
    cooking_duration_seconds: Optional[float]
    delivery_duration_seconds: Optional[float]
    source: Optional[str]


class OrderEventMessage(TypedDict):
    """Message schema for order-events topic."""
    order_id: str
    timestamp: str
    event_type: str
    actor: str
    order_number: int
    customer_name: str
    total_pizzas: int
    metadata: OrderEventMetadata


# ============================================================================
# Event Type Definitions
# ============================================================================

class EventType(str, Enum):
    """Order lifecycle event types."""
    ORDER_CREATED = "order_created"
    COOKING_STARTED = "cooking_started"
    COOKING_COMPLETED = "cooking_completed"
    DELIVERY_STARTED = "delivery_started"
    DELIVERY_COMPLETED = "delivery_completed"


class OrderSource(str, Enum):
    """Order source types."""
    WEB = "web"
    FRONTDESK = "frontdesk"


# ============================================================================
# Helper Functions for Message Creation
# ============================================================================

def generate_order_id() -> str:
    """Generate a unique order ID (UUID)."""
    return str(uuid.uuid4())


def get_current_timestamp() -> str:
    """Get current timestamp in ISO 8601 UTC format."""
    return datetime.now(timezone.utc).isoformat()


def select_random_pizza(weights: Dict[str, float] = None) -> str:
    """
    Select a random pizza based on complexity distribution weights.
    
    Args:
        weights: Distribution weights for pizza complexity
                 Default: {"simple": 0.50, "medium": 0.35, "complex": 0.15}
    
    Returns:
        Random pizza type name
    """
    if weights is None:
        weights = {"simple": 0.50, "medium": 0.35, "complex": 0.15}
    
    # Choose complexity level based on weights
    complexity_choice = random.choices(
        population=["simple", "medium", "complex"],
        weights=[weights["simple"], weights["medium"], weights["complex"]],
        k=1
    )[0]
    
    # Select random pizza from chosen complexity
    if complexity_choice == "simple":
        return random.choice(SIMPLE_PIZZAS)
    elif complexity_choice == "medium":
        return random.choice(MEDIUM_PIZZAS)
    else:
        return random.choice(COMPLEX_PIZZAS)


def generate_pizza_list(num_pizzas: int = None) -> List[PizzaItem]:
    """
    Generate a list of pizzas for an order.
    
    Args:
        num_pizzas: Number of pizzas in order. If None, randomly selected
                    based on distribution (1: 50%, 2: 35%, 3: 15%)
    
    Returns:
        List of PizzaItem dictionaries
    """
    if num_pizzas is None:
        num_pizzas = random.choices(
            population=[1, 2, 3],
            weights=[0.50, 0.35, 0.15],
            k=1
        )[0]
    
    pizzas: List[PizzaItem] = []
    for _ in range(num_pizzas):
        pizza_type = select_random_pizza()
        pizzas.append({"pizza_type": pizza_type, "quantity": 1})
    
    return pizzas


def calculate_total_pizzas(pizzas: List[PizzaItem]) -> int:
    """Calculate total number of pizzas from pizza items list."""
    return sum(item["quantity"] for item in pizzas)


def calculate_cooking_time(pizzas: List[PizzaItem], time_multiplier: float = 1.0) -> float:
    """
    Calculate total cooking time for an order.
    
    Args:
        pizzas: List of pizza items in the order
        time_multiplier: Multiplier for cooking times (for speed adjustment)
    
    Returns:
        Total cooking time in seconds
    """
    total_time = 0.0
    for item in pizzas:
        pizza = PIZZA_MENU.get(item["pizza_type"])
        if pizza:
            total_time += pizza.cooking_time * item["quantity"]
    return total_time * time_multiplier


def create_order_message(
    source: str,
    customer_name: str,
    order_number: int,
    pizzas: List[PizzaItem] = None,
    order_id: str = None,
) -> OrderMessage:
    """
    Create a pizza order message.
    
    Args:
        source: Order source (web|frontdesk)
        customer_name: Customer name
        order_number: Sequential order number
        pizzas: List of pizza items (auto-generated if None)
        order_id: Order UUID (auto-generated if None)
    
    Returns:
        OrderMessage dictionary
    """
    if order_id is None:
        order_id = generate_order_id()
    if pizzas is None:
        pizzas = generate_pizza_list()
    
    return OrderMessage(
        order_id=order_id,
        timestamp=get_current_timestamp(),
        source=source,
        customer_name=customer_name,
        pizzas=pizzas,
        total_pizzas=calculate_total_pizzas(pizzas),
        order_number=order_number,
    )


def create_cooked_pizza_message(
    original_order: OrderMessage,
    cooked_by: str,
    cooking_duration_seconds: float,
) -> CookedPizzaMessage:
    """
    Create a cooked pizza message from an order.
    
    Args:
        original_order: Original order message
        cooked_by: ID of the pizza maker who cooked the order
        cooking_duration_seconds: Actual cooking duration
    
    Returns:
        CookedPizzaMessage dictionary
    """
    return CookedPizzaMessage(
        order_id=original_order["order_id"],
        timestamp=get_current_timestamp(),
        cooked_by=cooked_by,
        cooked_at=get_current_timestamp(),
        cooking_duration_seconds=cooking_duration_seconds,
        customer_name=original_order["customer_name"],
        pizzas=original_order["pizzas"],
        total_pizzas=original_order["total_pizzas"],
        order_number=original_order["order_number"],
        original_order=original_order,
    )


def create_order_event_message(
    order_id: str,
    event_type: str,
    actor: str,
    order_number: int,
    customer_name: str,
    total_pizzas: int,
    metadata: OrderEventMetadata = None,
) -> OrderEventMessage:
    """
    Create an order event message.
    
    Args:
        order_id: Order UUID
        event_type: Type of event (order_created, cooking_started, etc.)
        actor: Component that generated the event
        order_number: Sequential order number
        customer_name: Customer name
        total_pizzas: Total number of pizzas in order
        metadata: Optional metadata dictionary
    
    Returns:
        OrderEventMessage dictionary
    """
    if metadata is None:
        metadata = OrderEventMetadata()
    
    return OrderEventMessage(
        order_id=order_id,
        timestamp=get_current_timestamp(),
        event_type=event_type,
        actor=actor,
        order_number=order_number,
        customer_name=customer_name,
        total_pizzas=total_pizzas,
        metadata=metadata,
    )


# ============================================================================
# Customer Name Generator
# ============================================================================

FIRST_NAMES = [
    "John", "Jane", "Michael", "Emily", "David", "Sarah", "Chris", "Lisa",
    "Robert", "Jessica", "William", "Mary", "James", "Patricia", "Daniel",
    "Linda", "Matthew", "Barbara", "Joseph", "Susan", "Anthony", "Karen",
    "Mark", "Nancy", "Paul", "Betty", "Andrew", "Helen", "Joshua", "Dorothy"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Thompson", "White", "Harris", "Sanchez", "Clark", "Lewis"
]


def generate_customer_name() -> str:
    """Generate a random customer name."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"
