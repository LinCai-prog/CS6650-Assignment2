package model;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class LeaderboardUpdateTest {
 @Test void shouldRoundTripZeroVersionSnapshotButRejectItAsChange() {
  var snapshot=new LeaderboardUpdate("snapshot","3","1",0,null,null,List.of());
  assertEquals(0,LeaderboardUpdate.fromSnapshot(snapshot.toSnapshotJson()).getVersion());
  assertThrows(IllegalArgumentException.class,()->LeaderboardUpdate.fromJson(snapshot.toJson()));
 }
 @Test void shouldShareExactCapturedPayloadBetweenProducerAndIngest() {
  var update=new LeaderboardUpdate("e","3","1",2,"2026-11-09T00:00:00Z",null,
    List.of(new AbstractMap.SimpleImmutableEntry<>("1234",800L)));
  assertEquals(update.toJson(),LeaderboardUpdate.fromJson(update.toJson()).toJson());
 }
}
