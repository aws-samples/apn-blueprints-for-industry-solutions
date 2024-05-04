from aws_cdk import (
     NestedStack
)
from constructs import Construct
import aws_cdk.aws_kinesis as kinesis
from dif.stacks.simulator.simulator.c360simulator.util import loadConfig

config = loadConfig()


class SimulatorDataStreamStack(NestedStack):
    '''
    DataStreams in simulator are the exposed endpoints for producer consumer pattern
    '''

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        '''
        A simple stack which instantiates few data streams
        
        Simulator write streams. Simulator would publish events here. 
        1. Customer Action Stream - Actions such as Search, View Item, Add to Cart, Purchase, Review, Support Chat Message, Register
        2. Product Action Stream - Add Product, Add Stock, 
        
        Simulator read streams. Simulator would listen to response messages here  
        1. Search Response Stream - Response to action type "search" with list of 10 matching search results (possibly reranked by customer personalization)
        2. Recommendation Response Stream - Respond to - recommend similar, recommend user, rerank items

        '''
        super().__init__(scope, id, **kwargs)
        
        # random_suffix = "-"+util.get_random_string(15)
        

        self.click_stream  = kinesis.Stream(self, "C360SimulatorClickStream",
            stream_name=config['streams']['customer-action'],
            stream_mode=kinesis.StreamMode.ON_DEMAND
        )
        

