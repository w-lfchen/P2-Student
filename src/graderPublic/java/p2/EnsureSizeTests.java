package p2;

import org.junit.jupiter.params.ParameterizedTest;
import org.junitpioneer.jupiter.json.JsonClasspathSource;
import org.junitpioneer.jupiter.json.Property;
import org.sourcegrade.jagr.api.rubric.TestForSubmission;
import org.tudalgo.algoutils.tutor.general.assertions.Context;
import p2.btrfs.BtrfsFile;
import p2.btrfs.BtrfsNode;
import p2.btrfs.IndexedNodeLinkedList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.assertEquals;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.call;
import static org.tudalgo.algoutils.tutor.general.assertions.Assertions2.contextBuilder;
import static p2.TreeUtil.assertTreeEquals;
import static p2.TreeUtil.constructTree;
import static p2.TreeUtil.getRoot;
import static p2.TreeUtil.treeToString;

@SuppressWarnings("DuplicatedCode")
@TestForSubmission
public class EnsureSizeTests {

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testNoChange")
    public void testNoChange(@Property("tree") List<Object> tree,
                             @Property("degree") int degree,
                             @Property("parentIndex") int parentIndex,
                             @Property("childIndex") int childIndex) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, parentIndex, -1, childIndex, -1, tree, null);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testRotateRight")
    public void testRotateRight(@Property("tree") List<Object> tree,
                                @Property("degree") int degree,
                                @Property("parentIndex") int parentIndex,
                                @Property("childIndex") int childIndex,
                                @Property("expectedParentIndex") int expectedParentIndex,
                                @Property("expectedChildIndex") int expectedChildIndex,
                                @Property("expected") List<Object> expected) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex, -1, expectedChildIndex, -1, expected, null);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testRotateLeft")
    public void testRotateLeft(@Property("tree") List<Object> tree,
                               @Property("degree") int degree,
                               @Property("parentIndex") int parentIndex,
                               @Property("childIndex") int childIndex,
                               @Property("expectedParentIndex") int expectedParentIndex,
                               @Property("expectedChildIndex") int expectedChildIndex,
                               @Property("expected") List<Object> expected) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex, -1, expectedChildIndex, -1, expected, null);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testRotateBoth")
    public void testRotateBoth(@Property("tree") List<Object> tree,
                               @Property("degree") int degree,
                               @Property("parentIndex") int parentIndex,
                               @Property("childIndex") int childIndex,
                               @Property("expectedParentIndex") int expectedParentIndex,
                               @Property("expectedChildIndex1") int expectedChildIndex1,
                                 @Property("expectedChildIndex2") int expectedChildIndex2,
                               @Property("expected1") List<Object> expected1,
                               @Property("expected2") List<Object> expected2) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex, expectedParentIndex, expectedChildIndex1, expectedChildIndex2, expected1, expected2);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testMergeLeft")
    public void testMergeLeft(@Property("tree") List<Object> tree,
                              @Property("degree") int degree,
                              @Property("parentIndex") int parentIndex,
                              @Property("childIndex") int childIndex,
                              @Property("expectedParentIndex") int expectedParentIndex,
                              @Property("expectedChildIndex") int expectedChildIndex,
                              @Property("expected") List<Object> expected) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex, -1, expectedChildIndex, -1, expected, null);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testMergeRight")
    public void testMergeRight(@Property("tree") List<Object> tree,
                               @Property("degree") int degree,
                               @Property("parentIndex") int parentIndex,
                               @Property("childIndex") int childIndex,
                               @Property("expectedParentIndex") int expectedParentIndex,
                               @Property("expectedChildIndex") int expectedChildIndex,
                               @Property("expected") List<Object> expected) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex, -1, expectedChildIndex, -1, expected, null);
    }

    @ParameterizedTest
    @JsonClasspathSource(value = "EnsureSizeTests.json", data = "testMergeBoth")
    public void testMergeBoth(@Property("tree") List<Object> tree,
                              @Property("degree") int degree,
                              @Property("parentIndex") int parentIndex,
                              @Property("childIndex") int childIndex,
                              @Property("expectedParentIndex1") int expectedParentIndex1,
                              @Property("expectedParentIndex2") int expectedParentIndex2,
                              @Property("expectedChildIndex1") int expectedChildIndex1,
                              @Property("expectedChildIndex2") int expectedChildIndex2,
                              @Property("expected1") List<Object> expected1,
                              @Property("expected2") List<Object> expected2) throws Throwable {

        testEnsureSize(tree, degree, parentIndex, childIndex, expectedParentIndex1, expectedParentIndex2, expectedChildIndex1, expectedChildIndex2, expected1, expected2);
    }

    private void testEnsureSize(List<Object> tree,
                                int degree,
                                int parentIndex,
                                int childIndex,
                                int expectedParentIndex1,
                                int expectedParentIndex2,
                                int expectedChildIndex1,
                                int expectedChildIndex2,
                                List<Object> expected1,
                                List<Object> expected2) throws Throwable {

        Context.Builder<?> context = contextBuilder()
            .subject("BtrfsFile.ensureSize()")
            .add("tree", treeToString(tree))
            .add("degree", degree)
            .add("parentIndex", parentIndex)
            .add("childIndex", childIndex);

        if (expected2 == null) {

            context.add("expectedParentIndex", expectedParentIndex1);
            context.add("expectedChildIndex1", expectedChildIndex1);
            context.add("expected", treeToString(expected1));
        } else {
            context.add("expectedParentIndex1", expectedParentIndex1);
            context.add("expectedChildIndex1", expectedChildIndex1);
            context.add("expected variant1", treeToString(expected1));

            context.add("expectedParentIndex2", expectedParentIndex2);
            context.add("expectedChildIndex2", expectedChildIndex2);
            context.add("expected variant2", treeToString(expected2));
        }

        TreeUtil.FileAndStorage actualFileAndStorage = constructTree(tree, degree);
        BtrfsFile actualTree = actualFileAndStorage.file();
        BtrfsNode root = getRoot(actualTree);
        int initialSize = actualTree.getSize();

        IndexedNodeLinkedList indexedRoot = new IndexedNodeLinkedList(null, root, parentIndex);
        IndexedNodeLinkedList indexedChild = childIndex == -1 ? null : new IndexedNodeLinkedList(indexedRoot, root.children[parentIndex], childIndex);

        call(() -> callEnsureSize(actualTree, childIndex == -1 ? indexedRoot : indexedChild), context.build(),
            TR -> "ensureSize() should not throw an exception");

        context.add("actual tree", treeToString(actualTree, actualFileAndStorage.storage()));

        int matchingTreeIndex = -1;
        AssertionError error = null;
        String message = expected2 == null ? "The tree is not correct" : "The tree does not match any of the expected tree variants";

        for (int i = 0; i < 2; i++) {

            List<Object> expected = i == 0 ? expected1 : expected2;

            if (expected == null) {
                continue;
            }

            TreeUtil.FileAndStorage expectedFileAndStorage = constructTree(expected, degree);
            BtrfsFile expectedTree = expectedFileAndStorage.file();

            try {
                assertTreeEquals(context.build(), message, getRoot(expectedTree),
                    getRoot(actualTree), expectedFileAndStorage.storage(), actualFileAndStorage.storage());

                matchingTreeIndex = i;
                break;
            } catch (AssertionError e) {
                error = e;
            }
        }

        if (matchingTreeIndex == -1) {
            throw error;
        }

        assertEquals(initialSize, actualTree.getSize(), context.build(),
            TR -> "The size of the tree should not change");

        assertEquals(matchingTreeIndex == 0 ? expectedParentIndex1 : expectedParentIndex2, indexedRoot.index, context.build(),
            TR -> "The index of the parent is not correct");
        assertEquals(root, indexedRoot.node, context.build(),
            TR -> "The node of the parent should not change");
        assertEquals(null, indexedRoot.parent, context.build(),
            TR -> "The parent of the parent should not change");

        if (indexedChild != null) {
            assertEquals(matchingTreeIndex == 0 ? expectedChildIndex1 : expectedChildIndex2, indexedChild.index, context.build(),
                TR -> "The index of the child is not correct");
            assertEquals(root.children[matchingTreeIndex == 0 ? expectedParentIndex1 : expectedParentIndex2], indexedChild.node, context.build(),
                TR -> "The node of the child should not change");
            assertEquals(indexedRoot, indexedChild.parent, context.build(),
                TR -> "The parent of the child should not change");
        }
    }

    private void callEnsureSize(BtrfsFile tree, IndexedNodeLinkedList indexedNode) throws Exception {

        Method method = BtrfsFile.class.getDeclaredMethod("ensureSize", IndexedNodeLinkedList.class);
        method.setAccessible(true);
        try {
            method.invoke(tree, indexedNode);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            } else {
                throw e;
            }
        }
    }

}
