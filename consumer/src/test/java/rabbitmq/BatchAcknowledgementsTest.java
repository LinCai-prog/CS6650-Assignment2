package rabbitmq;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class BatchAcknowledgementsTest {
    @Test void neverAcknowledgesPastAnUncommittedDelivery() throws Exception {
        var channel=mock(Channel.class);when(channel.isOpen()).thenReturn(true);
        try(var acks=new BatchAcknowledgements(channel,25,10000)) {
            acks.register(1);acks.register(2);acks.register(3);acks.committed(3);acks.flush();
            verify(channel,never()).basicAck(anyLong(),anyBoolean());
            acks.committed(1);acks.flush();verify(channel).basicAck(1,true);
            acks.committed(2);acks.flush();verify(channel).basicAck(3,true);
        }
    }
    @Test void sparseCommittedDeliveryFlushesOnTimer() throws Exception {
        var channel=mock(Channel.class);when(channel.isOpen()).thenReturn(true);
        try(var acks=new BatchAcknowledgements(channel,25,5)) {
            acks.register(1);acks.committed(1);verify(channel,timeout(1000)).basicAck(1,true);
        }
    }
    @Test void recoveryClearsOldTagsWithoutAcknowledgingTheirNewCopiesEarly() throws Exception {
        var channel=mock(Channel.class);when(channel.isOpen()).thenReturn(true);
        try(var acks=new BatchAcknowledgements(channel,25,10000)) {
            var listener=ArgumentCaptor.forClass(ShutdownListener.class);verify(channel).addShutdownListener(listener.capture());
            acks.register(1);listener.getValue().shutdownCompleted(mock(ShutdownSignalException.class));
            acks.register(101);acks.committed(1);acks.flush();verify(channel,never()).basicAck(anyLong(),anyBoolean());
            acks.committed(101);acks.flush();verify(channel).basicAck(101,true);
        }
    }
}
