package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

// 使用了LRU算法
public class Cache<K, V> {

    private HashMap<K, Node> cache;
    private int capacity;
    private Node head;
    private Node tail;

    //为了便于删除节点，使用了双端链表
    private class Node {
        Node front;
        Node next;
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public Cache(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>(capacity);
        this.head = new Node(null, null);
        this.tail = null;
    }

    private void delete(Node node) {
        if (node.next == null) {
            node.front.next = null;
        } else {
            node.front.next = node.next;
            node.next.front = node.front;
        }
    }

    private void insertFirst(Node node) {
        Node first = head.next;
        head.next = node;
        node.front = head;
        node.next = first;

        if (first == null) {
            tail = node;
        } else {
            first.front = node;
        }
    }

    private K deleteTail() {
        K element = tail.key;
        Node newTail = tail.front;
        // 消除游离
        tail.front = null;
        newTail.next = null;
        tail = newTail;
        return element;
    }

    public V put(K key, V value) {
        if (key == null | value == null) {
            throw new IllegalArgumentException();
        }

        if (cache.containsKey(key)) {
            Node node = cache.get(key);
            node.value = value;
            delete(node);
            insertFirst(node);
            return null;
        } else {
            V removed = null;
            if (cache.size() == capacity) {
                K removedKey = deleteTail();
                removed = cache.remove(removedKey).value;
            }
            Node node = new Node(key, value);
            insertFirst(node);
            cache.put(key, node);
            return removed;
        }
    }

    public V get(K key) {
        if (cache.containsKey(key)) {
            // 将节点放到链表前面
            Node node = cache.get(key);
            delete(node);
            insertFirst(node);
            return node.value;
        }
        return null;
    }

    public Iterator<V> iterator() {
        return new LruIter();
    }

    private class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public boolean hasNext() {
            return n.next != null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n = n.next;
            return n.value;
        }
    }
}