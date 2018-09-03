package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

// 用链表实现LRU算法
public class LruMap<K, V> {

    // 使用双端链表便于删除
    private class Node {
        Node pre;
        Node next;
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    // 只有put和remove会修改map
    private HashMap<K, Node> keyToNode;
    private int capacity;
    private Node head;
    private Node tail;
    private Node cur;

    public LruMap(int capacity) {
        this.capacity = capacity;
        this.keyToNode = new HashMap<K, Node>(capacity);
        this.head = new Node(null, null);
        this.tail = null;
        this.cur = null;
    }


    public void put(K key, V value) {
        if (key == null | value == null) {
            throw new IllegalArgumentException();
        }

        if (keyToNode.containsKey(key)) {
            Node node = keyToNode.get(key);
            node.value = value;
            delete(node);
            insertFirst(node);
        } else {
            Node node = new Node(key, value);
            insertFirst(node);
            keyToNode.put(key, node);
        }
    }

    public boolean isFull() {
        return keyToNode.size() == this.capacity;
    }

    /**
     * @return 最应该被移除的值
     */
    public V getCur() {
        cur = tail;
        return cur.value;
    }

    /**
     * @return 另一个可以被移除的值
     */
    public V getAnother() {
        cur = cur.pre;
        return cur == head ? null : cur.value;
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

    private void delete(Node node) {
        if (node.next == null) {
            node.pre.next = null;
            tail = node.pre;
        } else {
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
    }

    /**
     * 移除可以被移除的值
     */
    public void removeCur() {
        this.keyToNode.remove(cur.key);
        delete(cur);
        cur = tail;
    }

    public V get(K key) {
        if (keyToNode.containsKey(key)) {
            // 将节点放到链表的前端
            Node node = keyToNode.get(key);
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