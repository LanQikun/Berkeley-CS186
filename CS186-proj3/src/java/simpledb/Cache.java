package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

// 使用了LRU算法，用链表实现
public class Cache<K, V> {

    private HashMap<K, Node> cache;
    private int capacity;
    private Node head;
    private Node tail;

    private class Node {
        // 由于需要在尾部删除，所以用了双端链表
        Node pre;
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

    private K deleteTail() {
        K element = tail.key;
        Node newTail = tail.pre;
        // 防止游离
        tail.pre = null;
        newTail.next = null;
        tail = newTail;
        return element;
    }

    public V add(K key, V value) {
        if (key == null | value == null) {
            throw new IllegalArgumentException();
        }

        if (cache.containsKey(key)) {
            Node node = cache.get(key);
            node.value = value;
            remove(node);
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

    private void insertFirst(Node node) {
        Node first = head.next;
        head.next = node;
        node.pre = head;
        node.next = first;

        if (first == null) {
            tail = node;
        } else {
            first.pre = node;
        }
    }

    // node之后还会被insert，所以不需要防止游离
    private void remove(Node node) {
        if (node.next == null) {
            node.pre.next = null;
        } else {
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
    }

    public V get(K key) {
        if (cache.containsKey(key)) {
            // 将节点放到链表的前端
            Node node = cache.get(key);
            remove(node);
            insertFirst(node);
            return node.value;
        }
        return null;
    }

    public Iterator<V> iterator() {
        return new LruIter();
    }

    private class LruIter implements Iterator<V> {
        Node n;

        LruIter() {
            n = head;
        }

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