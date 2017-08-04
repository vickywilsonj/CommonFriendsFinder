
# CommonFriendsFinder

This is a MapReduce application, which finds a common friends between two friends.

# Input:

* A->B C D
* B->A C D E
* C->A B D E
* D->A B C E
* E->B C D

# Output:

* A B      >      C D
* A C      >      B D
* A D      >      B C
* B C      >      A D E
* B D      >      A C E
* B E      >      C D
* C D      >      A B E
* C E      >      B D
* D E      >      B C
