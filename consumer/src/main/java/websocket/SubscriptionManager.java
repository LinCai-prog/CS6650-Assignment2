
package websocket;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionManager {

    private final ConcurrentHashMap<SubscriptionKey, Set<String>>
            subscriptionsByLeaderboard = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Set<SubscriptionKey>>
            subscriptionsBySession = new ConcurrentHashMap<>();

    private final Object lock = new Object();

    public void subscribe(
            String sessionId,
            SubscriptionKey key) {

        validateSessionId(sessionId);
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        synchronized (lock) {
            subscriptionsByLeaderboard
                    .computeIfAbsent(
                            key,
                            k -> ConcurrentHashMap.newKeySet()
                    )
                    .add(sessionId);

            subscriptionsBySession
                    .computeIfAbsent(
                            sessionId,
                            id -> ConcurrentHashMap.newKeySet()
                    )
                    .add(key);
        }
    }

    public void unsubscribe(
            String sessionId,
            SubscriptionKey key) {

        validateSessionId(sessionId);
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        synchronized (lock) {
            Set<String> subscribers =
                    subscriptionsByLeaderboard.get(key);

            if (subscribers != null) {
                subscribers.remove(sessionId);

                if (subscribers.isEmpty()) {
                    subscriptionsByLeaderboard.remove(key);
                }
            }

            Set<SubscriptionKey> keys =
                    subscriptionsBySession.get(sessionId);

            if (keys != null) {
                keys.remove(key);

                if (keys.isEmpty()) {
                    subscriptionsBySession.remove(sessionId);
                }
            }
        }
    }

    public Set<String> getSubscribers(SubscriptionKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        synchronized (lock) {
            Set<String> subscribers =
                    subscriptionsByLeaderboard.get(key);

            if (subscribers == null) {
                return new HashSet<>();
            }

            return new HashSet<>(subscribers);
        }
    }

    public void removeSession(String sessionId) {
        validateSessionId(sessionId);

        synchronized (lock) {
            Set<SubscriptionKey> keys =
                    subscriptionsBySession.remove(sessionId);

            if (keys == null) {
                return;
            }

            for (SubscriptionKey key : keys) {
                Set<String> subscribers =
                        subscriptionsByLeaderboard.get(key);

                if (subscribers != null) {
                    subscribers.remove(sessionId);

                    if (subscribers.isEmpty()) {
                        subscriptionsByLeaderboard.remove(key);
                    }
                }
            }
        }
    }

    public Set<SubscriptionKey> getSubscriptions(
            String sessionId) {

        validateSessionId(sessionId);

        synchronized (lock) {
            Set<SubscriptionKey> keys =
                    subscriptionsBySession.get(sessionId);

            if (keys == null) {
                return new HashSet<>();
            }

            return new HashSet<>(keys);
        }
    }

    private void validateSessionId(String sessionId) {
        if (sessionId == null || sessionId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Session ID cannot be empty"
            );
        }
    }
}
