package practice;

import java.util.Set;

public class Movie {
        private String name;
        private Set<String> genres;

        public Movie(String name, Set<String> genres) {
            this.name = name;
            this.genres = genres;
        }

        public String getName() {
            return name;
        }

        public Set<String> getGenres() {
            return genres;
        }
}
